#!/usr/bin/env python
# coding: utf-8

# In[ ]:



from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

import os
import re
import json
import pandas as pd
import psycopg2
import datetime
from google.cloud import storage

import requests

# ใช้สำหรับ upload หรือ download ไฟล์จาก google cloud storage (gcp) ที่ bucket:thanyaboon-bluepi-gcs
def upload_download_blob(blob_name, file_path, bucket_name, mode):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/thanyaboonjob/exam_bluepi/config/credential_connect.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if mode == "upload":
        blob.upload_from_filename(file_path)
    elif mode == "download":
        blob.download_to_filename(file_path)
    return blob


# ใช้สำหรับสร้าง dataframe ของข้อมูลที่ทำการ query มาจาก postgresql database
def create_raw_data(list_col_nm,data,filename):
    regex = re.compile(r'([a-z0-9\_]+)', re.I) 
    col_nm = regex.findall(list_col_nm) # ทำการดึงชื่อแต่ละ columns ของ table จากผลลัพธ์ที่ได้ทำการ query มาจาก function connect_postgres
    tmp_df = pd.DataFrame(data, columns = col_nm)
    c = datetime.datetime.now()
    tm = "{}_0{}_{}_{}:{}".format(c.year,c.month,c.day,c.hour+7,c.minute)
#   ชื่อไฟล์จะมี format ดังนี้ filename_year_month_day_hour:miniute เช่น users_2021-06-15_23:59.csv เป็นต้น
    tmp_df.to_csv("/home/thanyaboonjob/exam_bluepi/rawdata/{}_{}.csv".format(filename,tm))
    return "{}_{}.csv".format(filename,tm)

# ใช้สำหรับเชื่อมต่อกับฐานข้อมูล  postgresql database และทำการดึงข้อมูลจาก 2 tables ได้แก่ users และ user_log
def connect_postgres(tbl_nm):
    DB_HOST = "35.247.174.171"
    DB_PORT = "5432"
    DB_NAME = "postgres"
    DB_USER = "exam"
    DB_PASS = "bluePiExam"

    conn = psycopg2.connect(dbname=DB_NAME, port=DB_PORT, user=DB_USER, password=DB_PASS, host=DB_HOST)

    raw_data_name = "raw_data_{}".format(tbl_nm)
    data_name = "data_{}".format(tbl_nm)
    

    with conn:
            with conn.cursor() as cur:
                # query ชื่อ columns จาก table ที่ต้องการ
                cur.execute("SELECT column_name FROM information_schema.columns WHERE TABLE_NAME = '{}'".format(tbl_nm))
                col_nm = cur.fetchall()
                cur.execute("SELECT * from {};".format(tbl_nm))
                data = cur.fetchall()
    conn.close()
    return col_nm,data

# ใช้สำหรับเขียนชื่อของ file csv ไปยัง text file ชื่อ csv_filename
# ไฟล์ csv_filename จะมีหน้าที่ในการเก็บชื่อของไฟล์ csv (ไฟล์ข้อมูลดิบ) เพื่อที่เมื่อนำไฟล์ไปประมวลผลแล้วจะได้ สามารถสร้างไฟล์ผลลัพธ์ที่ตรงกัน 
# โดยไฟล์ผลลัพธ์สุดท้ายที่ได้จากการประมวลผลนั้นจะมีการเติมคำว่า data ขึ้นมาข้างหน้าไฟล์ข้อมูลดิบเท่านั้นเพื่อให้ได้ชื่อไฟล์ข้อมูลดิบที่ตรงกัน
# จึงจำเ็นที่จะต้อง บันทึกชื่อไฟล์ข้อมูลดิบไว้
def write_filename(name):
    create_filename = open("/home/thanyaboonjob/exam_bluepi/config/csv_filename.txt","a")
    create_filename.write("{}\n".format(name))
    create_filename.close()
    
# ใช้สำหรับลบข้อมูลในบรรทัดแรกของ text file csv_filename
def delete_filename():
    del_filename = open("/home/thanyaboonjob/exam_bluepi/config/csv_filename.txt", "r")
    lines = del_filename.readlines()
    del_filename.close()

    del lines[0]
    del_filename = open("/home/thanyaboonjob/exam_bluepi/config/csv_filename.txt", "w+")
    for line in lines:
        del_filename.write(line)
    del_filename.close()

# ใช้สำหรับดึงข้อมูลจาก postgres db สร้างเป็นไฟล์ csv แะ upload ไปยัง bucket บน cloud storage
# โดยมีการเรียกใช้งานผ่าน 4 functions ได้แก่ connect_postgres, create_raw_data, upload_download_blob และ write_filename
def create_file_to_cloud():
    col_nm,data = connect_postgres("users")
    fn = create_raw_data(str(col_nm),data,"users")
    upload_download_blob("RAWDATA/PROFILE/USERS/{}".format(fn),"/home/thanyaboonjob/exam_bluepi/rawdata/{}".format(fn),"thanyaboon-bluepi-gcs","upload")
    
    col_nm,data = connect_postgres("user_log")
    fn = create_raw_data(str(col_nm),data,"user_log")
    upload_download_blob("RAWDATA/LOG/USERLOG/{}".format(fn),"/home/thanyaboonjob/exam_bluepi/rawdata/{}".format(fn),"thanyaboon-bluepi-gcs","upload")
    write_filename(fn)
    
# ใช้สำหรับประมวลผลข้อมูลดิบตามเงื่อนไที่กำหนด โดยเงื่อนไขจะประกอบด้วย
# 1. ทำการเปลี่ยนแปลงข้อมูลจาก 1 ให้เป็น True 0 เป็น False และ เปลี่ยนชื่อ column เป็น Success
# 2. เปลี่ยนชนิดของการเก็บข้อมูลเป็น Boolean
def transform():
#     อ่านไฟล์ config ที่เก็บข้อมูลอยู่ในรูปแบบของ json ซึ่งภายใน file จะมีข้อมูลของชื่อ table ชื่อ column ที่ต้องการจะเปลี่ยน และ ต้องการจะเปลี่ยนแปลง
#     ค่าของ column จาก ค่าหนึ่งไปยังอีกค่าหนึ่ง
    f = open('/home/thanyaboonjob/exam_bluepi/config/config.json')
    conf = json.load(f)
    
#     อ่านชื่อของไฟล์ข้อมูลดิบที่จะนำมาทำการประมวลผล
    read_filename = open("/home/thanyaboonjob/exam_bluepi/config/csv_filename.txt", "r")
    csv_name = read_filename.readline()
    csv_name = csv_name.replace("\n","")
    read_filename.close()
    if csv_name == '\n':
        delete_filename()
        read_filename = open("/home/thanyaboonjob/exam_bluepi/config/csv_filename.txt", "r")
        csv_name = read_filename.readline()
        csv_name = csv_name.replace("\n","")
        read_filename.close()
#     download ไฟล์ข้อมูลดิบจาก cloud storage
    upload_download_blob("RAWDATA/LOG/USERLOG/{}".format(csv_name), "/home/thanyaboonjob/exam_bluepi/preprocess/{}".format(csv_name), "thanyaboon-bluepi-gcs", "download")
    raw_data = pd.read_csv("/home/thanyaboonjob/exam_bluepi/preprocess/{}".format(csv_name))
    raw_data = raw_data.iloc[: , 1:]
#     ใช้สำหรับ return ค่าที่ต้องการจะเปลี่ยน โดยค่าที่ต้องการจะเปลี่ยนจะถูกบันทึกอยู่ใน file config.json
    def condition(tf_data,tbl_nm):
        key = []
        for k in conf[tbl_nm]['change_val'][0]:
            key.append(k)

        if str(tf_data) in key:
            return conf[tbl_nm]['change_val'][0][str(tf_data)]
        else:
            return tf_data
#      เปลี่ยนแปลงข้อมูลจาก string ให้เป็น boolean
    def str_to_bool(s):
        if s == 'True':
             return True
        elif s == 'False':
             return False
        else:
             return s

    tbl_nm = "user_log"
    filename = "data_{}".format(csv_name)

    raw_data[conf[tbl_nm]['column_name']] = raw_data[conf[tbl_nm]['column_name']].apply(condition,args=(tbl_nm,))
    raw_data[conf[tbl_nm]['column_name']] = raw_data[conf[tbl_nm]['column_name']].apply(str_to_bool)
#     เปลี่ยนชื่อ column 
    raw_data = raw_data.rename({conf[tbl_nm]['column_name']:str(conf[tbl_nm][conf[tbl_nm]['column_name']])}, axis=1)
#     สร้างไฟล์ข้อมูลที่ทำการประมวลจากข้อมูลดิบเรียบร้อยแล้ว
    raw_data.to_csv("/home/thanyaboonjob/exam_bluepi/finaldata/{}".format(filename))
    raw_data.to_csv("/home/thanyaboonjob/exam_bluepi/finaldata/{}.csv".format("USER_LOG_LASTEST"))
#     ทำการ upload ไฟล์ผลลัพธ์ไปยัง bucket บน cloud storage
    upload_download_blob("CURATEDATA/LOG/USERLOG/{}".format(filename),"/home/thanyaboonjob/exam_bluepi/finaldata/{}".format(filename),"thanyaboon-bluepi-gcs","upload")
    upload_download_blob("CURATEDATA/LOG/USERLOG/{}.csv".format("USER_LOG_LASTEST"),"/home/thanyaboonjob/exam_bluepi/finaldata/{}.csv".format("USER_LOG_LASTEST"),"thanyaboon-bluepi-gcs","upload")
#     ลบชื่อไฟล์ที่ประมวลผลเรียบร้อยแล้วจากไฟล์ csv_filename.txt
    delete_filename()


default_args = {
    'owner': 'exam-of-bluepi',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# สร้าง DAG โดยให้รันทุกๆ ชั่วโมง
dag = DAG(
    'exam-bluepi-1.10',
    default_args=default_args,
    description='ETL DAG',
    schedule_interval='0 */1 * * *',
)



t1 = PythonOperator(
    task_id='get_write_raw_data',
    python_callable=create_file_to_cloud,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_raw_data',
    python_callable=transform,
    dag=dag,

)

# ดึงข้อมูลจาก cloud storage ไปยัง Bigquery 
t3 = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='thanyaboon-bluepi-gcs',
        source_objects=['/CURATEDATA/LOG/USERLOG/{}.csv'.format("USER_LOG_LASTEST")],
        destination_project_dataset_table='LOG.P_USER_LOG',
        schema_fields=[
                {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'action', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'Success', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                ],
                write_disposition='WRITE_TRUNCATE',
                dag=dag)

t1 >> t2 >> t3

