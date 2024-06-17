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
#kakao ego_pose 테이블 데이터 삽입
def kakao_ego_pose_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao ego_pose 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/ego_pose.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)
                data=[]

                for ego_pose_data in json_data:

                    uuid=ego_pose_data['uuid']
                    token=uuid.replace("-","")

                    timestamp=ego_pose_data['timestamp']
                    translation_x=ego_pose_data['translation'][0]
                    translation_y=ego_pose_data['translation'][1]
                    translation_z=ego_pose_data['translation'][2]
                    rotation_q0=ego_pose_data['rotation'][0]
                    rotation_q1=ego_pose_data['rotation'][1]
                    rotation_q2=ego_pose_data['rotation'][2]
                    rotation_q3=ego_pose_data['rotation'][3]
                    coordinate_system=ego_pose_data['coordinate_system']

                    if coordinate_system==None:
                        data.append((token, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, coordinate_system))

                    else:
                        data.append((token, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, json.dumps(coordinate_system)))

                query="INSERT IGNORE INTO ego_pose (token, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, coordinate_system) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes ego_pose 테이블 데이터 삽입
def nuScenes_ego_pose_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/ego_pose.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for ego_pose_data in json_data:
                token=ego_pose_data['token']
                timestamp=str(ego_pose_data['timestamp'])
                rotation_q0=ego_pose_data['rotation'][0]
                rotation_q1=ego_pose_data['rotation'][1]
                rotation_q2=ego_pose_data['rotation'][2]
                rotation_q3=ego_pose_data['rotation'][3]
                translation_x=ego_pose_data['translation'][0]
                translation_y=ego_pose_data['translation'][1]
                translation_z=ego_pose_data['translation'][2]
                
                data.append((token, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3))

            query="INSERT IGNORE INTO ego_pose (token, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_ego_pose_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_ego_pose_insert=PythonOperator(task_id='kakao_ego_pose_insert', python_callable=kakao_ego_pose_insert)
    nuScenes_ego_pose_insert=PythonOperator(task_id='nuScenes_ego_pose_insert', python_callable=nuScenes_ego_pose_insert)


[kakao_ego_pose_insert,nuScenes_ego_pose_insert]