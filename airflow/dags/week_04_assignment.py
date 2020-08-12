import psycopg2
import requests

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


dag_second_assignment = DAG(
	dag_id = "second_assignment",
	start_date = datetime(2020,8,10), # 적당히 조절
	schedule_interval = "@daily"
    )  # 적당히 조절

# Redshift connection 함수
def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = ""
    redshift_pass = ""
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(**kwargs):
    f = requests.get(kwargs["url"])
    return (f.text)

def transform(**kwargs):
    text = kwargs["ti"].xcom_pull(key="return_value", task_ids="perform_extract")
    lines = text.split("\n")
    return lines

def load(**kwargs):
    cur = get_Redshift_connection()
    cur.execute("""BEGIN; 
                   TRUNCATE TABLE ysjune1051.name_gender; 
                   END;""")
    lines = kwargs["ti"].xcom_pull(key="return_value", task_ids="perform_transform")
    lines = iter(lines)
    next(lines) # header 건너뛰기
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(name, "-", gender)
            sql = """BEGIN; 
                     INSERT INTO ysjune1051.name_gender VALUES ('{name}', '{gender}'); 
                     END;""".format(name=name, gender=gender)
            print(sql)
            cur.execute(sql)

task_extract = PythonOperator(
	task_id = "perform_extract",
	python_callable = extract, 
    op_kwargs={"url": "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"}, 
    provide_context=True,  
	dag = dag_second_assignment
    )

task_transform = PythonOperator(
	task_id = "perform_transform",
	python_callable = transform, 
    provide_context=True,  
	dag = dag_second_assignment
    )

task_load = PythonOperator(
	task_id = "perform_load",
	python_callable = load, 
    provide_context=True, 
	dag = dag_second_assignment
    )

task_extract >> task_transform >> task_load