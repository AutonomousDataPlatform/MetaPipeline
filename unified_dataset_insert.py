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
#kakao dataset 테이블 데이터 삽입
def kakao_dataset_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                uuid=json_data['uuid']
                token=uuid.replace("-","")

                log_uuid=json_data['log_uuid']
                log_token=log_uuid.replace("-","")

                name=json_data['name']
                num_frames=json_data['num_frames']

                first_frame_uuid=json_data['first_frame_uuid']
                first_frame_token=first_frame_uuid.replace("-","")

                last_frame_uuid=json_data['last_frame_uuid']
                last_frame_token=last_frame_uuid.replace("-","")

                data=(token, log_token, name, num_frames, first_frame_token, last_frame_token)

                query="INSERT IGNORE INTO dataset (token, log_token, name, num_frames, first_frame_token, last_frame_token) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)




#nuScenes dataset 테이블 데이터 삽입
def nuScenes_dataset_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/scene.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for scene_data in json_data:
                token=scene_data['token']
                log_token=scene_data['log_token']
                num_frames=scene_data['nbr_samples']
                first_frame_token=scene_data['first_sample_token']
                last_frame_token=scene_data['last_sample_token']
                name=scene_data['name']
                
                data.append((token, log_token, num_frames, first_frame_token,last_frame_token, name))
                print(data)

            query="INSERT IGNORE INTO dataset (token, log_token, num_frames, first_frame_token,last_frame_token, name) VALUES (%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_dataset_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_dataset_insert=PythonOperator(task_id='kakao_dataset_insert', python_callable=kakao_dataset_insert)
    nuScenes_dataset_insert=PythonOperator(task_id='nuScenes_dataset_insert', python_callable=nuScenes_dataset_insert)


[kakao_dataset_insert,nuScenes_dataset_insert]