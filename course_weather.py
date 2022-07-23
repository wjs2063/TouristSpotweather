import json
import pathlib
import airflow
from urllib import request
import requests
import requests.exceptions as request_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import datetime as dt
from pathlib import Path
import pandas as pd
import pendulum
import os.path
from airflow.operators.dummy import DummyOperator
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

API_KEY="비밀"







dag=DAG(
    dag_id='download_weather_information',
    start_date=dt.datetime(2022,7,20,12,0,0),
    schedule_interval=dt.timedelta(hours=1),
)

#start=DummyOperator(task_id="start")

#output_path="/Users/pn_jh/Desktop/DockerProjects/weather/json_test"
#1일전부터 시작
#Path(output_path).mkdir(exist_ok=True)
#url=f"https://apis.data.go.kr/1360000/TourStnInfoService/getTourStnVilageFcst?serviceKey={API_KEY}&pageNo=1&numOfRows=1000&dataType=JSON&CURRENT_DATE={2022070110}&HOUR=1&COURSE_ID={1}"
#request.urlretrieve(url,output_path+"/today.json")

def _get_course_weather(year,month,day,hour,output_path):
    Path(output_path).mkdir(exist_ok=True)
    c_id=[i for i in range(1,438+1)]
    for course_id in c_id:
        Path(output_path+f"/{year}-{month}-{day}").mkdir(exist_ok=True)
        url=f"https://apis.data.go.kr/1360000/TourStnInfoService/getTourStnVilageFcst?serviceKey={API_KEY}&pageNo=1&numOfRows=1000&dataType=JSON&CURRENT_DATE={year}{month:>02}{day:>02}{hour:>02}&HOUR=1&COURSE_ID={course_id}"
        request.urlretrieve(url,output_path+f"/{year}-{month}-{day}"+f"/{year}-{month}-{day}-{hour}.json")
    return


get_course_weather_data=PythonOperator(
    task_id="get_course_weater_data",
    python_callable=_get_course_weather,
    op_kwargs={"year":"{{execution_date.year}}",
               "month":"{{execution_date.month}}",
               "day":"{{execution_date.day}}",
               "hour":"{{execution_date.hour}}",
               "output_path":"/opt/airflow/logs/json_test"
        },
    dag=dag
    )
get_course_weather_data
