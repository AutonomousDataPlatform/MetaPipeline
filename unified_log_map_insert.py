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
#kakao log_map 테이블 데이터 삽입
def kakao_log_map_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/log.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                log_uuid=json_data['uuid']
                log_token=log_uuid.replace("-","")

                map_uuids=json_data['map_uuids']

                if map_uuids==None:
                    map_token=None
                    data.append((log_token, map_token))

                
                #log 테이블의 map_uuids 가 null 값이 아닌 배열인 경우를 위해
                else:
                    for map_uuid in map_uuids:
                        #uuid 형식을 token 형식으로 변환해주어서 저장해야함
                        map_token=map_uuid.replace("-","")
                        data.append((log_token,map_token))
                

                query="INSERT IGNORE INTO log_map (log_token, map_token) VALUES (%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes log_map 테이블 데이터 삽입
def nuScenes_log_map_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/map.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for map_data in json_data:
                map_token=map_data['token']
                log_tokens=map_data['log_tokens']

                for log_token in log_tokens:
                    data.append((log_token, map_token))
            

            query="INSERT IGNORE INTO log_map (log_token, map_token) VALUES (%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_log_map_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_log_map_insert=PythonOperator(task_id='kakao_log_map_insert', python_callable=kakao_log_map_insert)
    nuScenes_log_map_insert=PythonOperator(task_id='nuScenes_log_map_insert', python_callable=nuScenes_log_map_insert)


[kakao_log_map_insert,nuScenes_log_map_insert]