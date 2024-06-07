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


remote_path='/data/3D_object_detection_tracking(autonomous_driving)'


#frame 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def frame_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='frame.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/frame.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)

                data=[]
                for frame_data in json_data:
                    data.append((frame_data['uuid'],frame_data['timestamp'],frame_data['dataset_uuid'],frame_data['prev'],frame_data['next']))
                    
                query="INSERT IGNORE INTO frame (uuid, timestamp, dataset_uuid, prev, next) VALUES (%s,%s,%s,%s,%s)"
                    
                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_frame_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    frame_insert=PythonOperator(task_id='frame_insert', python_callable=frame_insert)


frame_insert