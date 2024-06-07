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
#lidarseg 테이블 데이터 삽입
def lidarseg_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes_lidarseg/v1.0-mini'
    local_path='lidarseg.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/lidarseg.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for lidarseg_data in json_data:
                token=lidarseg_data['token']
                sample_data_token=lidarseg_data['sample_data_token']
                filename=lidarseg_data['filename']
                
                data.append((token, sample_data_token, filename))

            query="INSERT IGNORE INTO lidarseg (token, sample_data_token, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
            
    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_lidarseg_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    lidarseg_insert=PythonOperator(task_id='lidarseg_insert', python_callable=lidarseg_insert)



lidarseg_insert