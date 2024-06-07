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
#ego_pose 테이블 데이터 삽입
def ego_pose_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/ego_pose.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                data=[]

                for ego_pose_data in json_data:
                    data.append((ego_pose_data['uuid'],ego_pose_data['timestamp'],
                                 ego_pose_data['translation'][0],ego_pose_data['translation'][1],
                                 ego_pose_data['translation'][2],ego_pose_data['rotation'][0],
                                 ego_pose_data['rotation'][1],ego_pose_data['rotation'][2],
                                 ego_pose_data['rotation'][3],ego_pose_data['coordinate_system']))

                query="INSERT IGNORE INTO ego_pose (uuid, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, coordinate_system) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_ego_pose_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    ego_pose_insert=PythonOperator(task_id='ego_pose_insert', python_callable=ego_pose_insert)


ego_pose_insert