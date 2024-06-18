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
nuScenes_remote_path='/data/nuScenes_lidarseg/v1.0-mini'

#category 테이블 data 생성 함수 (상하관계 나타나는 path 포함된)
def generate_data(category_data, parent_path="", data=None):
    if data is None:
        data = []

    if isinstance(category_data, list):
        for item in category_data:
            
            generate_data(item, parent_path, data)
            
    elif isinstance(category_data, dict):

        name=category_data['value']
        description=category_data['description']
        
        default_scale=category_data.get("default_scale")

        #상하관계를 나타내기 위한 경로 생성
        if parent_path:
            path = f"{parent_path}.{name}"
        else:
            path = name

        
        #default_scale 가 none 값인 경우 db에 json 문자열로 변환된 형태인 'null'이 아닌 자체 NULL 값으로 들어가게 하기 위함
        if default_scale == None:

            #path 가 name 컬럼임 (name 이 상하관계를 내포하고 있어서 임의로 path 라고 네이밍)
            data.append((path, description, default_scale))

        else:
            #data.append((token, path, description, default_scale))
            data.append((path, description, json.dumps(default_scale)))
        

        children = category_data.get("children", [])
        for child in children:
            generate_data(child, path, data)

    return data


#sftp connection 을 이용하여 파일 다운로드
#kakao category 테이블 데이터 삽입
def kakao_category_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_category.json'

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

            # 이미 존재하는 name(즉, path) 목록 가져오기
            cur.execute("SELECT name FROM category")
            existing_names = {row[0] for row in cur.fetchall()}

            with open(kakao_local_path) as f:

                json_data=json.load(f)
                category_data=json_data['category']['contents']

                data=generate_data(category_data)

                # 중복 데이터 필터링
                filtered_data = []
                for entry in data:

                    path, description, default_scale = entry[0], entry[1], entry[2]

                    if path not in existing_names:
                        token = secrets.token_hex(16)
                        filtered_data.append((token, path, description, default_scale))
                
                query="INSERT IGNORE INTO category (token, name, description, default_scale) VALUES (%s,%s,%s,%s)"
                print(filtered_data)
                cur.executemany(query, filtered_data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes category 테이블 데이터 삽입
def nuScenes_category_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_category.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/category.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for category_data in json_data:
                
                token=category_data['token']
                name=category_data['name']
                description=category_data['description']
                index=category_data['index']
                
                data.append((token, name, description, index))

            query="INSERT IGNORE INTO category (token, name, description, `index`) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_category_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_category_insert=PythonOperator(task_id='kakao_category_insert', python_callable=kakao_category_insert)
    nuScenes_category_insert=PythonOperator(task_id='nuScenes_category_insert', python_callable=nuScenes_category_insert)


[kakao_category_insert,nuScenes_category_insert]