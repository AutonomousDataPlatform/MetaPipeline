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
#kakao frame_data 테이블 데이터 삽입
def kakao_frame_data_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_frame_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/frame_data.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                for frame_data_data in json_data:

                    uuid=frame_data_data['uuid']
                    token=uuid.replace("-","")

                    frame_uuid=frame_data_data['frame_uuid']
                    frame_token=frame_uuid.replace("-","")

                    ego_pose_uuid=frame_data_data['ego_pose_uuid']
                    ego_pose_token=ego_pose_uuid.replace("-","")

                    sensor_uuid=frame_data_data['sensor_uuid']
                    sensor_token=sensor_uuid.replace("-","")

                    timestamp=frame_data_data['timestamp']
                    is_key_frame=frame_data_data['is_key_frame']

                    if frame_data_data['height']==None:
                        height=0
                    else:
                        height=frame_data_data['height']

                    if frame_data_data['width']==None:
                        width=0
                    else:
                        width=frame_data_data['width']
                    
                    file_name=frame_data_data['file_name']
                    file_format=frame_data_data['file_format']
                    prev=frame_data_data['prev']
                    next=frame_data_data['next']

                    data.append((token, frame_token, ego_pose_token, sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next))


                query="INSERT IGNORE INTO frame_data (token, frame_token, ego_pose_token, sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes frame_data 테이블 데이터 삽입
def nuScenes_frame_data_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_sample_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sample_data.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for sample_data_data in json_data:

                token=sample_data_data['token']
                frame_token=sample_data_data['sample_token']
                ego_pose_token=sample_data_data['ego_pose_token']
                sensor_token=sample_data_data['calibrated_sensor_token']
                timestamp=str(sample_data_data['timestamp'])
                is_key_frame=sample_data_data['is_key_frame']
                height=sample_data_data['height']
                width=sample_data_data['width']
                file_name=sample_data_data['filename']
                file_format=sample_data_data['fileformat']

                if sample_data_data['prev']=="":
                    prev=None
                else:
                    prev=sample_data_data['prev']

                if sample_data_data['next']=="":
                    next=None
                else:
                    next=sample_data_data['next']


                data.append((token, frame_token, ego_pose_token, sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next))

            query="INSERT IGNORE INTO frame_data (token, frame_token, ego_pose_token, sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_frame_data_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_frame_data_insert=PythonOperator(task_id='kakao_frame_data_insert', python_callable=kakao_frame_data_insert)
    nuScenes_frame_data_insert=PythonOperator(task_id='nuScenes_frame_data_insert', python_callable=nuScenes_frame_data_insert)


[kakao_frame_data_insert,nuScenes_frame_data_insert]