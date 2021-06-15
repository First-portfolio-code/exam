from pyspark.sql import SparkSession
import os 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/thanyaboonjob/exam_bluepi/config/credential_connect.json'
spark = SparkSession \
    .builder \
    .appName('spark-read-from-bigquery') \
    .getOrCreate()

df = spark.read.format('bigquery') \
    .option('project','thanyaboon-bluepi-de-exam') \
    .option('table','DATATANK.test') \
    .load()

print(df.schema)

df.show()

# spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.1 /home/thanyaboonjob/exam_bluepi/code/pyspark-bigquery-example.py
# ใช้ command ข้างต้นในการรันบนหน้า shell