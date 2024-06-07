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


#category_name 최하위 요소 추출 함수
def get_lowest_value(category_name):
    if isinstance(category_name, str):
        
        return category_name.split('.')[-1]
    else:
        return category_name

#instance 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def instance_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/instance.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                data=[]

                for instance_data in json_data:
                    category_name=get_lowest_value(instance_data['category_name'])
                    data.append((instance_data['uuid'],
                                category_name,
                                instance_data['first_frame_annotation_uuid'],
                                instance_data['last_frame_annotation_uuid'],
                                instance_data['num_annotations'],
                                instance_data['description']))

                query="INSERT IGNORE INTO instance (uuid, category_name, first_frame_annotation_uuid, last_frame_annotation_uuid, num_annotations, description) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)




with DAG('kakao_mobility_instance_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    instance_insert=PythonOperator(task_id='instance_insert', python_callable=instance_insert)


instance_insert