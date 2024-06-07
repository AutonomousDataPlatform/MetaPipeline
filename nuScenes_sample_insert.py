import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import os


default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


#sftp connection 을 이용하여 파일 다운로드
#sample 테이블 데이터 삽입
def sample_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_data in json_data:
                token=sample_data['token']
                timestamp=sample_data['timestamp']
                prev=sample_data['prev']
                next=sample_data['next']
                scene_token=sample_data['scene_token']
                
                data.append((token, timestamp, prev, next, scene_token))

            query="INSERT IGNORE INTO sample (token, timestamp, prev, next, scene_token) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_sample_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    sample_insert=PythonOperator(task_id='sample_insert', python_callable=sample_insert)



sample_insert