import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import os
import secrets


default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


kakao_remote_path='/data/3D_object_detection_tracking(autonomous_driving)'
nuScenes_remote_path='/data/nuScenes/v1.0-mini'



#sftp connection 을 이용하여 파일 다운로드
#kakao sensor 테이블 데이터 삽입
def kakao_sensor_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/sensor.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            # 이미 존재하는 channel = name 목록 가져오기
            cur.execute("SELECT channel FROM sensor")
            existing_names = {row[0] for row in cur.fetchall()}

            with open(kakao_local_path) as f:

                json_data=json.load(f)
                data=[]

                for sensor_data in json_data:

                    channel=sensor_data['name']
                    modality=sensor_data['type']
                    
                    data.append((channel,modality))


                # 중복 데이터 필터링
                filtered_data = []

                #중복된 name (camero(00)....)이 존재하지 않는 경우에만 token 생성후 데이터 삽입
                for entry in data:

                    channel, modality = entry[0], entry[1]

                    if channel not in existing_names:
                        token = secrets.token_hex(16)
                        filtered_data.append((token, channel, modality))
                
                query="INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"
                
                cur.executemany(query, filtered_data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes sensor 테이블 데이터 삽입
def nuScenes_sensor_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sensor.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]
            
            for sensor_data in json_data:
        
                token=sensor_data['token']
                channel=sensor_data['channel']
                modality=sensor_data['modality']

                data.append((token, channel, modality))

            query="INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_sensor_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_sensor_insert=PythonOperator(task_id='kakao_sensor_insert', python_callable=kakao_sensor_insert)
    nuScenes_sensor_insert=PythonOperator(task_id='nuScenes_sensor_insert', python_callable=nuScenes_sensor_insert)


[kakao_sensor_insert,nuScenes_sensor_insert]