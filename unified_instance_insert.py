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
#kakao instance 테이블 데이터 삽입
def kakao_instance_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    category_name_list=[]
    category_dict={}

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/instance.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                for instance_data in json_data:

                    uuid=instance_data['uuid']
                    token=uuid.replace("-","")

                    category_name=instance_data['category_name']
                    sql="SELECT token FROM category WHERE name=%s"
                    param=(category_name,)
                    cur.execute(sql, param)
                    result=cur.fetchone()

                    if result is not None:
                        category_token=result[0]
                    else:
                        category_token=None
                    
                    # category 테이블에 없는 category 일 때, token 생성해서 category 테이블에 추가
                    if category_token==None:

                        if category_name not in category_name_list:
                            category_name_list.append(category_name)
                            token = secrets.token_hex(16)
                            category_dict[category_name]=token

                        category_token=category_dict[category_name]
                        

                    first_frame_annotation_uuid=instance_data['first_frame_annotation_uuid']
                    first_frame_annotation_token=first_frame_annotation_uuid.replace("-","")

                    last_frame_annotation_uuid=instance_data['last_frame_annotation_uuid']
                    last_frame_annotation_token=last_frame_annotation_uuid.replace("-","")

                    num_annotations=instance_data['num_annotations']


                    data.append((token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations))

                query="INSERT IGNORE INTO instance (token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations) VALUES (%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

        # category 테이블에 빠져있는 데이터 삽입
        category_data=[]
        for category_name in category_name_list:
            category_data.append((category_dict[category_name], category_name))

        query="INSERT IGNORE INTO category (token, name) VALUES (%s,%s)"

        cur.executemany(query,category_data)
        conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes instance 테이블 데이터 삽입
def nuScenes_instance_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/instance.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for instance_data in json_data:

                token=instance_data['token']
                category_token=instance_data['category_token']
                first_frame_annotation_token=instance_data['first_annotation_token']
                last_frame_annotation_token=instance_data['last_annotation_token']
                num_annotations=instance_data['nbr_annotations']
                
                data.append((token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations))

            query="INSERT IGNORE INTO instance (token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_instance_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_instance_insert=PythonOperator(task_id='kakao_instance_insert', python_callable=kakao_instance_insert)
    nuScenes_instance_insert=PythonOperator(task_id='nuScenes_instance_insert', python_callable=nuScenes_instance_insert)


[kakao_instance_insert,nuScenes_instance_insert]