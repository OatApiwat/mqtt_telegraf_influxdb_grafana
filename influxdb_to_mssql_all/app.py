import time
import datetime
import pymssql
from influxdb import InfluxDBClient
from datetime import timedelta
import paho.mqtt.client as mqtt

# ==========================
# 🔹 CONFIGURATION SETTINGS
# ==========================
INFLUXDB_HOST = 'influxdb'
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = 'iot_data'
INFLUXDB_MEASUREMENT = 'mqtt_consumer'
INFLUXDB_MQTT_TOPICS = ['iot/data_1', 'iot/data_2']  # 🔥 รองรับหลาย Topic

MSSQL_SERVER = '192.168.0.128'
MSSQL_USER = 'sa'
MSSQL_PASSWORD = 'sa@admin'
MSSQL_DATABASE = 'iot_db'

INTERVAL = 1  # เก็บข้อมูลทุก 1 นาที
DELAY = 5      # หน่วงเวลา 5 วินาที ก่อนดึงข้อมูล

# ==========================
# 🔹 CONNECT TO INFLUXDB
# ==========================
influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
influx_client.switch_database(INFLUXDB_DATABASE)

# ==========================
# 🔹 CONNECT TO MSSQL
# ==========================
def connect_mssql():
    return pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DATABASE)

# ==========================
#  CONFIGURATION SETTINGS (เพิ่ม MQTT)
# ==========================
MQTT_BROKER = 'mosquitto'  # IP ของ MQTT Broker
MQTT_PORT = 1883
MQTT_TOPIC_CANNOT_INSERT = 'iot/cannot_insert'

# เชื่อมต่อ MQTT client
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

# ==========================
# 🔹 FUNCTION TO CREATE TABLES
# ==========================
def create_mssql_tables():
    """ สร้างตารางใน MSSQL ตาม MQTT Topics ถ้ายังไม่มี """
    conn = connect_mssql()
    cursor = conn.cursor()

    for topic in INFLUXDB_MQTT_TOPICS:
        table_name = topic.replace("/", "_").replace("-", "_") # เปลี่ยน '/' เป็น '_'
        table_name = f"raw_{table_name}"  # เพิ่มคำว่า "raw" นำหน้าชื่อ table
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            time DATETIME PRIMARY KEY,
            topic VARCHAR(255),
            data1 FLOAT,
            data2 FLOAT,
            data3 FLOAT,
            data4 FLOAT,
            data5 FLOAT,
            data6 FLOAT,
            data7 FLOAT,
            data8 FLOAT,
            data9 FLOAT,
            data10 FLOAT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"✅ Table '{table_name}' is ready.")

    cursor.close()
    conn.close()

# ==========================
# 🔹 FUNCTION TO FETCH DATA
# ==========================
def fetch_influxdb_data():
    """ ดึงข้อมูลจาก InfluxDB ตามช่วงเวลาที่กำหนด """
    now = datetime.datetime.utcnow()
    start_time = now - datetime.timedelta(minutes=1, seconds=now.second, microseconds=now.microsecond)
    end_time = start_time + datetime.timedelta(minutes=INTERVAL)

    all_data = {}
    for topic in INFLUXDB_MQTT_TOPICS:
        table_name = topic.replace('/', '_')  # เปลี่ยนชื่อ Table
        table_name = f"raw_{table_name}"  # เพิ่มคำว่า "raw" นำหน้าชื่อ table
        query = f"""
            SELECT * FROM "{INFLUXDB_MEASUREMENT}"
            WHERE time >= '{start_time.isoformat()}Z' AND time < '{end_time.isoformat()}Z'
            AND topic = '{topic}'
        """
        result = influx_client.query(query)
        all_data[table_name] = list(result.get_points())  # เก็บข้อมูลตาม Table

    return all_data

# ==========================
# 🔹 FUNCTION TO INSERT DATA
# ==========================
def insert_data_to_mssql(data):
    """ บันทึกข้อมูลลง MSSQL และตรวจสอบว่ามีอยู่ก่อนหรือไม่ """
    conn = connect_mssql()
    cursor = conn.cursor()

    for table_name, rows in data.items():
        for row in rows:
            # แปลงเวลาให้ตรงกับรูปแบบที่ MSSQL รองรับ
            timestamp = datetime.datetime.strptime(row['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
            # บวกเวลา 7 ชั่วโมง
            timestamp = timestamp + timedelta(hours=7)
            

            topic = row['topic']
            values = {key: row[key] for key in row if key not in ['time', 'topic', 'host']}

            # ตรวจสอบว่าข้อมูลซ้ำหรือไม่
            check_query = f"SELECT COUNT(*) FROM {table_name} WHERE time = %s AND topic = %s"
            cursor.execute(check_query, (timestamp, topic))
            count = cursor.fetchone()[0]

            if count == 0:  # ถ้ายังไม่มีข้อมูลนี้ใน MSSQL
                columns = ', '.join(['topic'] + list(values.keys()))
                placeholders = ', '.join(['%s'] * (len(values) + 1))
                insert_query = f"INSERT INTO {table_name} (time, {columns}) VALUES (%s, {placeholders})"
                try:
                    cursor.execute(insert_query, (timestamp, topic, *values.values()))
                    conn.commit()
                    
                    # ตรวจสอบว่ามีข้อมูลถูก insert เข้าไปจริงๆ หรือไม่
                    check_inserted_query = f"SELECT COUNT(*) FROM {table_name} WHERE time = %s AND topic = %s"
                    cursor.execute(check_inserted_query, (timestamp, topic))
                    inserted_count = cursor.fetchone()[0]

                    if inserted_count == 0:  # ถ้ายังไม่พบข้อมูลที่ถูก insert
                        raise Exception(f"Data not inserted properly for {timestamp} into {table_name}")
                    else:
                        print(f"✅ Inserted: {timestamp} | Table: {table_name}")
                except Exception as e:
                    conn.rollback()  # Rollback เมื่อเกิดข้อผิดพลาด
                    print(f"⚠️ Failed to insert: {timestamp} | Table: {table_name}")
                    # ส่งข้อมูลไปที่ MQTT topic iot/cannot_insert
                    mqtt_message = f"Failed to insert at {timestamp} into {table_name}. Error: {str(e)}"
                    mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, mqtt_message)
                    print(f"📡 Published to MQTT: {mqtt_message}")

            else:
                print(f"⚠️ Data already exists for: {timestamp} | Table: {table_name}")

    cursor.close()
    conn.close()

# ==========================
# 🔹 MAIN LOOP (EVERY 1 MIN)
# ==========================
def main():
    create_mssql_tables()  # สร้างตารางก่อนเริ่มทำงาน

    while True:
        time.sleep(DELAY)  # รอให้ผ่านไป 5 วินาที

        try:
            influx_data = fetch_influxdb_data()
            if influx_data:
                # print('ok')
                insert_data_to_mssql(influx_data)
            else:
                print("❌ No new data found!")

        except Exception as e:
            print(f"🚨 Error: {e}")

        time.sleep(INTERVAL*60 - DELAY)  # รอให้ครบ 1 นาที

# ==========================
# 🔹 RUN THE SCRIPT
# ==========================
if __name__ == "__main__":
    main()