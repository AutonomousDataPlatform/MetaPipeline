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


#sftp connection 을 이용하여 파일 다운로드
#log 테이블 데이터 삽입
def log_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:

            file_path=remote_path + '/' + dir_name + '/meta/log.json'
            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                log_data=json.load(f)

                if ('device_wlh' in log_data):
                    device_width=log_data['device_wlh'][0]
                    device_length=log_data['device_wlh'][1]
                    device_height=log_data['device_wlh'][2]
                else:
                    device_width=None
                    device_length=None
                    device_height=None

                data=(log_data['uuid'], log_data['map_uuids'], log_data['device'], log_data['date_captured'], log_data['location'], log_data['driving_distance'], device_width, device_length, device_height)

                query="INSERT IGNORE INTO log (uuid, map_uuids, device,date_captured, location, driving_distance, device_width, device_length, device_height) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_log_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    log_insert=PythonOperator(task_id='log_insert', python_callable=log_insert)


log_insert