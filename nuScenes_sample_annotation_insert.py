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
#sample_annotation 테이블 데이터 삽입
def sample_annotation_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample_annotation.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_annotation_data in json_data:
                token=sample_annotation_data['token']
                sample_token=sample_annotation_data['sample_token']
                instance_token=sample_annotation_data['instance_token']
                visibility_token=sample_annotation_data['visibility_token']

                if sample_annotation_data['attribute_tokens']:
                    attribute_tokens=sample_annotation_data['attribute_tokens'][0]
                else:
                    attribute_tokens=None
                
                translation_center_x=sample_annotation_data['translation'][0]
                translation_center_y=sample_annotation_data['translation'][1]
                translation_center_z=sample_annotation_data['translation'][2]
                size_width=sample_annotation_data['size'][0]
                size_length=sample_annotation_data['size'][1]
                size_height=sample_annotation_data['size'][2]
                rotation_w=sample_annotation_data['rotation'][0]
                rotation_x=sample_annotation_data['rotation'][1]
                rotation_y=sample_annotation_data['rotation'][2]
                rotation_z=sample_annotation_data['rotation'][3]
                prev=sample_annotation_data['prev']
                next=sample_annotation_data['next']
                num_lidar_pts=sample_annotation_data['num_lidar_pts']
                num_radar_pts=sample_annotation_data['num_radar_pts']
                
                data.append((token, sample_token, instance_token, visibility_token, attribute_tokens, translation_center_x, translation_center_y, translation_center_z, size_width, size_length, size_height, rotation_w, rotation_x, rotation_y,rotation_z, prev, next, num_lidar_pts,num_radar_pts))

            query="INSERT IGNORE INTO sample_annotation (token, sample_token, instance_token, visibility_token, attribute_tokens, translation_center_x, translation_center_y, translation_center_z, size_width, size_length, size_height, rotation_w, rotation_x, rotation_y,rotation_z, prev, next, num_lidar_pts,num_radar_pts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_sample_annotation_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    sample_annotation_insert=PythonOperator(task_id='sample_annotation_insert', python_callable=sample_annotation_insert)



sample_annotation_insert