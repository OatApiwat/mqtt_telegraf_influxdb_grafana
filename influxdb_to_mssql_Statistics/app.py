import time
import datetime
import pymssql
import numpy as np
from influxdb import InfluxDBClient
from datetime import timedelta
from scipy import stats  # สำหรับ mode

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

INTERVAL = 1  # เก็บข้อมูลทุก 1 นาที
DELAY = 5     # หน่วงเวลา 5 วินาที ก่อนดึงข้อมูล

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
# FUNCTION TO CREATE STATISTICS TABLES
# ==========================
def create_statistics_tables():
    """ สร้างตารางสถิติใน MSSQL """
    conn = connect_mssql()
    cursor = conn.cursor()

    for topic in INFLUXDB_MQTT_TOPICS:
        table_name = f"statistics_iot_{INTERVAL}min_{topic.replace('/', '_')}"
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            time DATETIME,
            topic VARCHAR(255),
            data_number VARCHAR(255),
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
        print(f"✅ Statistics Table '{table_name}' is ready.")

    cursor.close()
    conn.close()

# ==========================
# FUNCTION TO CALCULATE AND INSERT STATISTICS
# ==========================
def calculate_and_insert_statistics(data):
    """ คำนวณและบันทึกสถิติลง MSSQL """
    conn = connect_mssql()
    cursor = conn.cursor()

    for topic, rows in data.items():
        if not rows:
            continue

        table_name = f"statistics_iot_{INTERVAL}min_{topic.replace('/', '_')}"
        numeric_columns = [f"data{i}" for i in range(1, 11)]
        
        for col in numeric_columns:
            values = [row[col] for row in rows if row[col] is not None]
            if not values:
                continue

            mean = np.mean(values)
            median = np.median(values)
            mode = stats.mode(values).mode[0] if len(values) > 0 else None
            max_val = np.max(values)
            min_val = np.min(values)
            range_val = max_val - min_val
            sd = np.std(values)
            variance = np.var(values)

            timestamp = datetime.datetime.strptime(rows[0]['time'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=7)

            insert_query = f"""
            INSERT INTO {table_name} (time, topic, data_number, mean, median, mode, max, min, range, sd, variance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, (timestamp, topic, col, mean, median, mode, max_val, min_val, range_val, sd, variance))
            conn.commit()
            print(f"✅ Inserted statistics for {col} in {table_name} at {timestamp}.")

    cursor.close()
    conn.close()

# ==========================
# MAIN LOOP (EVERY 1 MIN)
# ==========================
def main():
    create_statistics_tables()

    while True:
        time.sleep(DELAY)  # รอให้ผ่านไป 5 วินาที

        try:
            # Fetch raw data from InfluxDB
            now = datetime.datetime.utcnow()
            start_time = now - timedelta(minutes=1, seconds=now.second, microseconds=now.microsecond)
            end_time = start_time + timedelta(minutes=INTERVAL)

            all_data = {}
            for topic in INFLUXDB_MQTT_TOPICS:
                query = f"""
                SELECT * FROM "{INFLUXDB_MEASUREMENT}"
                WHERE time >= '{start_time.isoformat()}Z' AND time < '{end_time.isoformat()}Z'
                AND topic = '{topic}'
                """
                result = influx_client.query(query)
                all_data[topic] = list(result.get_points())

            calculate_and_insert_statistics(all_data)

        except Exception as e:
            print(f"\u26A1 Error: {e}")

        time.sleep(INTERVAL*60 - DELAY)  # รอให้ครบ 1 นาที

# ==========================
# RUN THE SCRIPT
# ==========================
if __name__ == "__main__":
    main()
