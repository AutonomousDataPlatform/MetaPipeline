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


#sftp connection 을 이용하여 파일 다운로드
#ego_pose 테이블 데이터 삽입
def ego_pose_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/ego_pose.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for ego_pose_data in json_data:
                token=ego_pose_data['token']
                timestamp=ego_pose_data['timestamp']
                rotation_w=ego_pose_data['rotation'][0]
                rotation_x=ego_pose_data['rotation'][1]
                rotation_y=ego_pose_data['rotation'][2]
                rotation_z=ego_pose_data['rotation'][3]
                translation_x=ego_pose_data['translation'][0]
                translation_y=ego_pose_data['translation'][1]
                translation_z=ego_pose_data['translation'][2]
                
                data.append((token, timestamp, rotation_w, rotation_x, rotation_y, rotation_z, translation_x, translation_y, translation_z))

            query="INSERT IGNORE INTO ego_pose (token, timestamp, rotation_w, rotation_x, rotation_y, rotation_z, translation_x, translation_y, translation_z) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_ego_pose_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    ego_pose_insert=PythonOperator(task_id='ego_pose_insert', python_callable=ego_pose_insert)



ego_pose_insert