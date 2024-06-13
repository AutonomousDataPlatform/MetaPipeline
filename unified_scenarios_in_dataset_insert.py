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
#kakao scenarios_in_dataset 테이블 데이터 삽입
def kakao_scenarios_in_dataset_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        #kakao scenario 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                if isinstance(json_data,list):
                    for dataset_data in json_data:

                        dataset_uuid=dataset_data['uuid']
                        #uuid를 nuScenes 의 token 형식으로 바꿈
                        dataset_token=dataset_uuid.replace("-","")

                        scenario_names=dataset_data['scenario_names']

                    for scenario_description in scenario_names:

                        data.append((dataset_token,scenario_description))
                else:
                    dataset_uuid=json_data['uuid']
                    dataset_token=dataset_uuid.replace("-","")

                    scenario_names=json_data['scenario_names']
                    for scenario_description in scenario_names:

                        data.append((dataset_token,scenario_description))

                query="INSERT IGNORE INTO scenarios_in_dataset (dataset_token,scenario_description) VALUES (%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes scenarios_in_dataset 테이블 데이터 삽입
def nuScenes_scenarios_in_dataset_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/scene.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for scene_data in json_data:
                scene_token=scene_data['token']
                scene_description=scene_data['description']

                data.extend((scene_token, item.strip()) for item in scene_description.split(','))


            query="INSERT IGNORE INTO scenarios_in_dataset (dataset_token,scenario_description) VALUES (%s,%s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



with DAG('unified_scenarios_in_dataset_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_scenarios_in_dataset_insert=PythonOperator(task_id='kakao_scenarios_in_dataset_insert', python_callable=kakao_scenarios_in_dataset_insert)
    nuScenes_scenarios_in_dataset_insert=PythonOperator(task_id='nuScenes_scenarios_in_dataset_insert', python_callable=nuScenes_scenarios_in_dataset_insert)


[kakao_scenarios_in_dataset_insert,nuScenes_scenarios_in_dataset_insert]