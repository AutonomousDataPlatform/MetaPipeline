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

        if ('default_scale' in category_data):
            default_scale_x=category_data['default_scale'][0]
            default_scale_y=category_data['default_scale'][1]
            default_scale_z=category_data['default_scale'][2]
        else:
            default_scale_x=None
            default_scale_y=None
            default_scale_z=None

        attribute=category_data.get("attribute")

        #상하관계를 나타내기 위한 경로 생성
        if parent_path:
            path = f"{parent_path}.{name}"
        else:
            path = name

        #attribute 가 none 값인 경우 db에 json 문자열로 변환된 형태인 'null'이 아닌 자체 NULL 값으로 들어가게 하기 위함
        if attribute == None:
            data.append((name,path,description,default_scale_x,default_scale_y,default_scale_z,attribute))
        else:
            data.append((name,path,description,default_scale_x,default_scale_y,default_scale_z,json.dumps(attribute)))
        

        children = category_data.get("children", [])
        for child in children:
            generate_data(child, path, data)

    return data

#category 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def category_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='category.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/preset.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                category_data=json_data['category']['contents']

                data=generate_data(category_data)
                query="INSERT IGNORE INTO category (name, path, description, default_scale_x, default_scale_y, default_scale_z, attribute) VALUES (%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_category_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    category_insert=PythonOperator(task_id='category_insert', python_callable=category_insert)


category_insert