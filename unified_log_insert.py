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


kakao_remote_path='/data/3D_object_detection_tracking(autonomous_driving)'
nuScenes_remote_path='/data/nuScenes/v1.0-mini'

#sftp connection 을 이용하여 파일 다운로드
#kakao log 테이블 데이터 삽입
def kakao_log_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao ego_pose 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/log.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:

                log_data=json.load(f)

                uuid=log_data['uuid']
                token=uuid.replace("-","")

                device=log_data['device']

                date_captured=log_data['date_captured']

                if log_data['location']=="":
                    location=None
                else:
                    location=log_data['location']

                driving_distance=log_data['driving_distance']

                if ('device_wlh' in log_data):
                    device_width=log_data['device_wlh'][0]
                    device_length=log_data['device_wlh'][1]
                    device_height=1.775
                else:
                    device_width=None
                    device_length=None
                    device_height=None

                data=(token, device, date_captured, location, driving_distance, device_width, device_length, device_height)

                query="INSERT IGNORE INTO log (token, device, date_captured, location, driving_distance, device_width, device_length, device_height) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes log 테이블 데이터 삽입
def nuScenes_log_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/log.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for log_data in json_data:
                token=log_data['token']
                device=log_data['vehicle']
                date_captured=log_data['date_captured']
                location=log_data['location']
                logfile=log_data['logfile']
                
                
                data.append((token, device, date_captured, location, logfile))

            query="INSERT IGNORE INTO log (token, device, date_captured, location, logfile) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_log_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_log_insert=PythonOperator(task_id='kakao_log_insert', python_callable=kakao_log_insert)
    nuScenes_log_insert=PythonOperator(task_id='nuScenes_log_insert', python_callable=nuScenes_log_insert)


[kakao_log_insert,nuScenes_log_insert]