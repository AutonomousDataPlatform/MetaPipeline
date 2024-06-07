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
#map 테이블 데이터 삽입
def map_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/map.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for map_data in json_data:
                token=map_data['token']
                category=map_data['category']
                filename=map_data['filename']
                log_tokens=map_data['log_tokens']
                
                data.append((token, category, filename, json.dumps(log_tokens)))

            query="INSERT IGNORE INTO map (token, category, filename, log_tokens) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_map_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    map_insert=PythonOperator(task_id='map_insert', python_callable=map_insert)



map_insert