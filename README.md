
<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://www.facebook.com/bluePiCoLtd/">
    <img src="readme/1585883803443.jpg" alt="Logo" width="100" height="100">
  </a>

  <h3 align="center">README</h3>

    
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

เป็นโปรเจคที่ใช้สำหรับการทำข้อสอบจากบริษัท bluePI Co., Ltd. โดยโปรเจคนี้จะมีการ implement data pipeline โดยใช้ Apache Airflow และ ภาษา python โดย เริ่มจาก ดึงข้อมูลจาก PostgreSQL Database และเก็บข้อมูลทีทำการ Query มาไว้บน Cloud Storage ในรูปแบบของ ไฟล์ csv และทำการ transform ข้อมูลตามเงื่อนไขที่กำหนดและทำการจัดเก็บข้อมูลที่ประมวลผลเรียบร้อยแล้วไว้ที่ Cloud Storage ในรูปแบบของไฟล์ csv และ ส่งไปยัง Bigquery เพื่อทำการแสดงผล 

### Built With

เทคโนโลยีหลักๆ ทั้งหมดที่ได้มีการนำมาใช้งานในโปรเจคนี้ได้แก่

* [Python](https://www.python.org)
* [Apache Airflow](https://airflow.apache.org)
* [Google Cloud Platform](https://cloud.google.com)
* [Jupyter Notebook](https://jupyter.org)




<!-- GETTING STARTED -->
## Getting Started

ก่อนเริ่มต้นการใช้งาน ควรจะต้องติดตั้ง library ที่จำเป็นต่อการใช้งานตามขั้นตอนต่อไปนี้

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.
* pip
```sh
 sudo apt update
 sudo apt install python-pip
```
* python library
```sh
 pip install psycopg2
 pip install pyspark
 pip install numpy
 pip install pandas
 pip install google-cloud-storage
```

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/First-portfolio-code/exam-bluepi.git
   ```




<!-- USAGE EXAMPLES -->
## Usage

 Code สามารถ download ข้อมูลจาก prosgresSQL Database และ ทำการประมวลผลข้อมูลตามที่โจทย์กำหนดรวมไปถึง upload ไฟล์ผลลัพธ์กลับไปยัง Cloud Storage โดยรายละเอียดของการทำงาน
 สามารถศึกษาได้จาก Comment ใน code  ถ้าต้องการจะเพิ่มจำนวน table ที่ใช้ในการประมวลผลสามารถเพิ่มได้ที่ไฟล์ config  [Code](https://github.com/First-portfolio-code/exam-bluepi/blob/main/code/airflow-pipeline.py)

 ขั้นตอนในการใช้งานสำหรับรัน code บน jupyter โดยไม่ใช้ Airflow (บนเครื่อง VM ที่ได้ทำการสร้างไว้)
 - ใช้คำสั่ง pipenv shell
 - ใช้คำสั่ง jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser &
 - เปิด web browser แล้วทำการกรอก external ip:port ที่ ช่องใส่ url
 - ใส่ token ที่ได้รับจากหน้า shell ไปที่ ช่อง input บนหน้า web ui
 - เปิดไฟล์เพื่อรัน code

 ขั้นตอนในการใช้งานสำหรับรัน code บน  Airflow (บนเครื่อง VM ที่ได้ทำการสร้างไว้)
 - รันไฟล์ python ใน folder dags/ ด้วยคำสั่ง python filename.py
 - ใช้คำสั่ง airflow webserver -p 8080
 - ใช้คำสั่ง airflow scheduler 
 - เปิด web browser แล้วทำการกรอก external ip:port ที่ ช่องใส่ url
 - กรอก username password ที่ได้สร้างเอาไว้
 - active dag เพื่อเริ่มใช้งาน


 
<!-- CONTACT -->
## Contact

Project Link: [https://github.com/First-portfolio-code/exam](https://github.com/First-portfolio-code/exam)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Install Apache Airflow](https://medium.com/grensesnittet/airflow-on-gcp-may-2020-cdcdfe594019)
* [Query Cloud Storage](https://cloud.google.com/bigquery/external-data-cloud-storage)
* [Upload object](https://cloud.google.com/storage/docs/uploading-objects)
* [Install Jupyter](https://www.digitalocean.com/community/tutorials/how-to-install-run-connect-to-jupyter-notebook-on-remote-server)





