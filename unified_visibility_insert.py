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
#kakao visibility 테이블 데이터 삽입
def kakao_visibility_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_visibility.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/preset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                
                json_data=json.load(f)
                data=[]

                preset_data=json_data['visibility']['contents']

                for visibility_data in preset_data:

                    if visibility_data['level']==1:

                        token="0"
                        level="v0"
                        description="visibility of whole object is 0%"

                    elif visibility_data['level']==2:

                        token="5"
                        level="v100"
                        description="visibility of whole object is 100%"
                
                    data.append((token, level, description))

                query="INSERT IGNORE INTO visibility (token, level, description) VALUES (%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes visibility 테이블 데이터 삽입
def nuScenes_visibility_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_visibility.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/visibility.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for visibility_data in json_data:
        
                token=visibility_data['token']
                level=visibility_data['level']
                description=visibility_data['description']

                data.append((token, level, description))

            query="INSERT IGNORE INTO visibility (token, level, description) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_visibility_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_visibility_insert=PythonOperator(task_id='kakao_visibility_insert', python_callable=kakao_visibility_insert)
    nuScenes_visibility_insert=PythonOperator(task_id='nuScenes_visibility_insert', python_callable=nuScenes_visibility_insert)


[kakao_visibility_insert,nuScenes_visibility_insert]