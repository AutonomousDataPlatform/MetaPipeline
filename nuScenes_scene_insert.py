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
#scene 테이블 데이터 삽입
def scene_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/scene.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for scene_annotation_data in json_data:
                token=scene_annotation_data['token']
                log_token=scene_annotation_data['log_token']
                nbr_samples=scene_annotation_data['nbr_samples']
                first_sample_token=scene_annotation_data['first_sample_token']
                last_sample_token=scene_annotation_data['last_sample_token']
                name=scene_annotation_data['name']
                description=scene_annotation_data['description']
                
                data.append((token, log_token, nbr_samples, first_sample_token,last_sample_token, name, description))

            query="INSERT IGNORE INTO scene (token, log_token, nbr_samples, first_sample_token,last_sample_token, name, description) VALUES (%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_scene_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    scene_insert=PythonOperator(task_id='scene_insert', python_callable=scene_insert)



scene_insert