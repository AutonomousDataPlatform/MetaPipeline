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
#kakao attribute 테이블 데이터 삽입
def kakao_attribute_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_attrbute.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    # name 결과를 저장할 리스트
    # 디렉토리 돌때마다 초기화되면 안되고, 유지되어야함
    name_list=[]

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/frame_annotation.json'

            print(f"{kakao_file_path} get start")
            sftp_client.get(kakao_file_path, kakao_local_path)
            print(f"{kakao_file_path} get end")

            with open(kakao_local_path) as f:

                print(f"{kakao_file_path} load start")
                json_data=json.load(f)
                print(f"{kakao_file_path} load end")


                print(len(json_data))

                for i in range(len(json_data)):

                    attribute_data = json_data[i]['attribute']

                    if attribute_data is not None:
                        # 'attribute' 키의 값에 대해 반복
                        for key, value in attribute_data.items():
                            # '_state'가 키에 포함되어 있는지 확인
                            if '_state' in key:
                                # '_state'를 제거
                                new_key = key.replace('_state', '')
                            else:
                                # '_state'가 없으면 그대로 사용
                                new_key = key
                            
                            # 키와 값을 연결하여 새로운 문자열 생성
                            new_name = f"{new_key}.{value}"
                            
                            # 새로운 이름이 name_list에 없을 경우에만 추가
                            if new_name not in name_list:
                                name_list.append(new_name)

        #insert 할 데이터 리스트
        data=[]

        #모든 frame_annotation 에 대해서 attribute name 을 중복없이 만들고, 마지막에 insert
        for name in name_list:
            token = secrets.token_hex(16)
            data.append((token, name))
        
        query="INSERT IGNORE INTO attribute (token, name) VALUES (%s,%s)"

        cur.executemany(query,data)
        conn.commit()

        # 중복 name 삭제 쿼리 (nuScenes 를 남김. nuScenes 는 description 이 있기 때문에)
        query="DELETE a1 FROM `attribute` a1 JOIN `attribute` a2 WHERE a1.name = a2.name AND a1.description IS NULL AND a2.description IS NOT NULL"
        cur.exequte(query)
        conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes attribute 테이블 데이터 삽입
def nuScenes_attribute_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_attribute.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/attribute.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]
            
            for attribute_data in json_data:
                token=attribute_data['token']
                name=attribute_data['name']
                description=attribute_data['description']

                data.append((token, name, description))

            query="INSERT IGNORE INTO attribute (token, name, description) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(nuScenes_local_path)


with DAG('unified_attribute_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_attribute_insert=PythonOperator(task_id='kakao_attribute_insert', python_callable=kakao_attribute_insert)
    nuScenes_attribute_insert=PythonOperator(task_id='nuScenes_attribute_insert', python_callable=nuScenes_attribute_insert)


[kakao_attribute_insert,nuScenes_attribute_insert]