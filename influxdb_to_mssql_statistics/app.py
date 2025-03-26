import time
import datetime
import pymssql
from influxdb import InfluxDBClient
from datetime import timedelta
import numpy as np
import paho.mqtt.client as mqtt
from scipy import stats

# ==========================
# CONFIGURATION SETTINGS
# ==========================
INFLUXDB_HOST = 'localhost'
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = 'iot_data'
INFLUXDB_MEASUREMENT = 'mqtt_consumer'
INFLUXDB_MQTT_TOPICS = ['iot/data_1', 'iot/data_2']

MSSQL_SERVER = '192.168.0.128'
MSSQL_USER = 'sa'
MSSQL_PASSWORD = 'sa@admin'
MSSQL_DATABASE = 'iot_db'

INTERVAL = 1
DELAY = 5

MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC_CANNOT_INSERT = 'iot/cannot_insert'

# ==========================
# CONNECT TO INFLUXDB
# ==========================
influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
influx_client.switch_database(INFLUXDB_DATABASE)

# ==========================
# CONNECT TO MSSQL
# ==========================
def connect_mssql():
    return pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DATABASE)

# ==========================
# CONNECT TO MQTT
# ==========================
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

# ==========================
# FUNCTION TO CREATE STATIC TABLES
# ==========================
def create_static_tables():
    """ สร้างตาราง static สำหรับเก็บข้อมูลสถิติ """
    conn = connect_mssql()
    cursor = conn.cursor()

    for topic in INFLUXDB_MQTT_TOPICS:
        table_name = f"static_iot_{INTERVAL}min_{topic.replace('/', '_')}"
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            time DATETIME,
            topic VARCHAR(255),
            data_number VARCHAR(10),
            mean FLOAT,
            median FLOAT,
            mode FLOAT,
            max FLOAT,
            min FLOAT,
            range FLOAT,
            sd FLOAT,
            variance FLOAT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"✅ Static table '{table_name}' is ready.")

    cursor.close()
    conn.close()

# ==========================
# FUNCTION TO CALCULATE STATISTICS
# ==========================
def calculate_statistics(rows, topic):
    """ คำนวณค่าสถิติจากข้อมูลในช่วงเวลา """
    statistics = []
    try:
        for i in range(1, 11):
            data_key = f'data{i}'
            values = [row[data_key] for row in rows if data_key in row]
            
            if values:
                mean = np.mean(values)
                median = np.median(values)
                mode = stats.mode(values, nan_policy='omit').mode[0] if len(values) > 0 else None
                max_value = np.max(values)
                min_value = np.min(values)
                range_value = max_value - min_value
                sd = np.std(values)
                variance = np.var(values)

                statistics.append({
                    'data_number': data_key,
                    'mean': mean,
                    'median': median,
                    'mode': mode,
                    'max': max_value,
                    'min': min_value,
                    'range': range_value,
                    'sd': sd,
                    'variance': variance
                })
    except Exception as e:
        mqtt_message = f"Error calculating statistics for {topic}: {str(e)}"
        mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, mqtt_message)
        print(f"⚠️ Published to MQTT: {mqtt_message}")
        raise

    return statistics

# ==========================
# FUNCTION TO INSERT STATISTICS
# ==========================
def insert_statistics_to_mssql(statistics, topic, time):
    """ บันทึกค่าทางสถิติลง MSSQL """
    conn = connect_mssql()
    cursor = conn.cursor()

    table_name = f"static_iot_{INTERVAL}min_{topic.replace('/', '_')}"
    try:
        for stat in statistics:
            insert_query = f"""
            INSERT INTO {table_name} (time, topic, data_number, mean, median, mode, max, min, range, sd, variance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                time, topic, stat['data_number'], stat['mean'], stat['median'], stat['mode'],
                stat['max'], stat['min'], stat['range'], stat['sd'], stat['variance']
            ))
            conn.commit()
    except Exception as e:
        mqtt_message = f"Error inserting statistics for {topic} at {time}: {str(e)}"
        mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, mqtt_message)
        print(f"⚠️ Published to MQTT: {mqtt_message}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# ==========================
# FUNCTION TO FETCH DATA
# ==========================
def fetch_influxdb_data():
    """ ดึงข้อมูลจาก InfluxDB """
    now = datetime.datetime.utcnow()
    start_time = now - datetime.timedelta(minutes=1, seconds=now.second, microseconds=now.microsecond)
    end_time = start_time + datetime.timedelta(minutes=INTERVAL)

    all_data = {}
    for topic in INFLUXDB_MQTT_TOPICS:
        query = f"""
            SELECT * FROM "{INFLUXDB_MEASUREMENT}"
            WHERE time >= '{start_time.isoformat()}Z' AND time < '{end_time.isoformat()}Z'
            AND topic = '{topic}'
        """
        result = influx_client.query(query)
        all_data[topic] = list(result.get_points())

    return all_data, start_time

# ==========================
# MAIN LOOP (EVERY 1 MIN)
# ==========================
def main():
    create_static_tables()

    while True:
        time.sleep(DELAY)

        try:
            influx_data, data_time = fetch_influxdb_data()

            for topic, rows in influx_data.items():
                if rows:
                    statistics = calculate_statistics(rows, topic)
                    insert_statistics_to_mssql(statistics, topic, data_time)
                    print(f"✅ Statistics for '{topic}' at {data_time} inserted successfully.")
                else:
                    print(f"❌ No data found for topic '{topic}' at {data_time}.")

        except Exception as e:
            print(f"⚠️ Error: {e}")

        time.sleep(INTERVAL * 60 - DELAY)

# ==========================
# RUN THE SCRIPT
# ==========================
if __name__ == "__main__":
    main()