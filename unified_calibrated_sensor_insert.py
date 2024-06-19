##nuScenes 데이터 camera_intrinsic [] 빈 배열이 경우 체크해서 null 처리

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
#kakao calibrated_sensor 테이블 데이터 삽입
def kakao_calibrated_sensor_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_calibrated_sensor.json'

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

            with open(kakao_local_path) as f:

                json_data=json.load(f)
                data=[]

                for calibrated_sensor_data in json_data:

                    uuid=calibrated_sensor_data['uuid']
                    token=uuid.replace("-","")

                    sensor_name=calibrated_sensor_data['name']
                    sql="SELECT token FROM sensor WHERE channel=%s"
                    param=(sensor_name,)
                    cur.execute(sql, param)
                    result=cur.fetchone()

                    sensor_token=result[0]
                    translation=calibrated_sensor_data['translation']
                    rotation=calibrated_sensor_data['rotation']
                    camera_intrinsic=calibrated_sensor_data['intrinsic']
                    height_from_ground=calibrated_sensor_data['height_from_ground']

                    if token=="13826d14e1014ecca56a7730f1c4eec9":
                        print(translation, camera_intrinsic)

                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                    if token=="13826d14e1014ecca56a7730f1c4eec9":
                        print(json.dumps(translation), json.dumps(camera_intrinsic))

                query="INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes calibrated_sensor 테이블 데이터 삽입
def nuScenes_calibrated_sensor_insert():
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

        nuScenes_file_path=nuScenes_remote_path + '/calibrated_sensor.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]
            
            for calibrated_sensor_data in json_data:

                token=calibrated_sensor_data['token']
                sensor_token=calibrated_sensor_data['sensor_token']
                translation=calibrated_sensor_data['translation']
                rotation=calibrated_sensor_data['rotation']
                height_from_ground=calibrated_sensor_data['translation'][2]
                
                if calibrated_sensor_data['camera_intrinsic']:
                    camera_intrinsic=calibrated_sensor_data['camera_intrinsic']

                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                else:
                    camera_intrinsic=None
                    
                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), camera_intrinsic, height_from_ground))
                                

            query="INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"


            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_calibrated_sensor_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_calibrated_sensor_insert=PythonOperator(task_id='kakao_calibrated_sensor_insert', python_callable=kakao_calibrated_sensor_insert)
    nuScenes_calibrated_sensor_insert=PythonOperator(task_id='nuScenes_calibrated_sensor_insert', python_callable=nuScenes_calibrated_sensor_insert)


[kakao_calibrated_sensor_insert,nuScenes_calibrated_sensor_insert]