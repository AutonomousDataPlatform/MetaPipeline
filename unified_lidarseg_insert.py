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

nuScenes_remote_path='/data/nuScenes_lidarseg/v1.0-mini'


#nuScenes lidarseg 테이블 데이터 삽입
def nuScenes_lidarseg_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/lidarseg.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]

            for lidarseg_data in json_data:

                token=lidarseg_data['token']
                frame_data_token=lidarseg_data['sample_data_token']
                filename=lidarseg_data['filename']
                
                data.append((token, frame_data_token, filename))

            query="INSERT IGNORE INTO lidarseg (token, frame_data_token, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_lidarseg_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:

    nuScenes_lidarseg_insert=PythonOperator(task_id='nuScenes_lidarseg_insert', python_callable=nuScenes_lidarseg_insert)


nuScenes_lidarseg_insert