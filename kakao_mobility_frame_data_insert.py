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


#frame_data 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def frame_data_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='frame_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/frame_data.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                data=[]
                for frame_data_data in json_data:
                    data.append((frame_data_data['uuid'],
                                frame_data_data['frame_uuid'],
                                frame_data_data['ego_pose_uuid'],
                                frame_data_data['sensor_uuid'],
                                frame_data_data['timestamp'],
                                frame_data_data['is_key_frame'],
                                frame_data_data['height'],
                                frame_data_data['width'],
                                frame_data_data['file_name'],
                                frame_data_data['file_format'],
                                frame_data_data['prev'],
                                frame_data_data['next'],
                                frame_data_data['prev_key_frame_data'],
                                frame_data_data['next_key_frame_data']))


                query="INSERT IGNORE INTO frame_data (uuid, frame_uuid, ego_pose_uuid, sensor_uuid, timestamp, is_key_frame, height, width, file_name, file_format, prev, next, prev_key_frame_data, next_key_frame_data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_frame_data_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    frame_data_insert=PythonOperator(task_id='frame_data_insert', python_callable=frame_data_insert)


frame_data_insert