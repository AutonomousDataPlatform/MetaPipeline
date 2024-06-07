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


#dataset 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def dataset_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)

                data=(json_data['uuid'],json_data['log_uuid'],json_data['name'],json_data['description'],json_data['base_path'],json_data['num_frames'],json_data['first_frame_uuid'],json_data['last_frame_uuid'])

                query="INSERT IGNORE INTO dataset (uuid, log_uuid, name, description, base_path, num_frames, first_frame_uuid, last_frame_uuid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)



with DAG('kakao_mobility_dataset_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    dataset_insert=PythonOperator(task_id='dataset_insert', python_callable=dataset_insert)


dataset_insert