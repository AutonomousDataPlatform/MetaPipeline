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



#scenario 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def scenario_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='scenario.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                json_data2=json_data['scenario_names']

                data=[]
                for scenario_data_name in json_data2:
                    data.append(scenario_data_name)

                query="INSERT IGNORE INTO scenario (name) VALUES (%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)



with DAG('kakao_mobility_scenario_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    scenario_insert=PythonOperator(task_id='scenario_insert', python_callable=scenario_insert)


scenario_insert