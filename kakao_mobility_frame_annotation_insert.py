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


#frame_annotation 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def frame_annotation_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='frame_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")
    
    with hook.get_conn() as sftp_client:
        
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/frame_annotation.json'
            
            print(f"{file_path} get start")
            sftp_client.get(file_path, local_path)
            print(f"{file_path} get end")

            with open(local_path) as f:

                print(f"{file_path} load start")
                json_data=json.load(f)
                print(f"{file_path} load end")

                data=[]

                print(len(json_data))

                for i in range(len(json_data)):

                    frame_annotation_data = json_data[i]

                    uuid=frame_annotation_data['uuid']
                    frame_data_uuid=frame_annotation_data['frame_data_uuid']
                    instance_uuid=frame_annotation_data['instance_uuid']
                    visibility_level=frame_annotation_data['visibility_level']
                    annotation_type_name=frame_annotation_data['annotation_type_name']
                    is_lidar_synced=frame_annotation_data['is_lidar_synced']

                    if (frame_annotation_data['geometry']==None):
                        geometry=None
                    else:
                        geometry=json.dumps(frame_annotation_data['geometry'])
                        
                    num_pts=frame_annotation_data['num_pts']

                    if (frame_annotation_data['attribute']==None):
                        attribute=None
                    else:
                        attribute=json.dumps(frame_annotation_data['attribute'])

                    frame_aligned=frame_annotation_data['frame_aligned']
                    description=frame_annotation_data['description']
                    prev=frame_annotation_data['prev']
                    next=frame_annotation_data['next']


                    data.append((uuid, frame_data_uuid,instance_uuid, visibility_level, annotation_type_name, is_lidar_synced, geometry, num_pts, attribute, frame_aligned, description, prev, next))
                    

                    #50000개 단위로 insert
                    if (i % 50000 == 0 and i>0):

                        print("50000 data append end")

                        query = "INSERT IGNORE INTO frame_annotation (uuid, frame_data_uuid, instance_uuid, visibility_level, annotation_type_name, is_lidar_synced, geometry, num_pts, attribute, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                        
                        print("%d executemany start" %(i//50000))
                        cur.executemany(query,data)
                        print("%d executemany end" %(i//50000))

                        data = []

                # 나머지 데이터 삽입 ( 마지막 50000개까지 안 채워진 데이터 )
                query = "INSERT IGNORE INTO frame_annotation (uuid, frame_data_uuid, instance_uuid, visibility_level, annotation_type_name, is_lidar_synced, geometry, num_pts, attribute, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                print("executemany start")
                cur.executemany(query,data)
                print("executemany end")

                #commit은 트랜잭션 고려해서 한 데이터셋에 대한 모든 데이터가 insert 됐을 때 마지막에 실행
                print("commit start")
                conn.commit()
                print("insert end")

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_frame_annotation_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    frame_annotation_insert=PythonOperator(task_id='frame_annotation_insert', python_callable=frame_annotation_insert)


frame_annotation_insert