import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import pymysql
import os


default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


remote_path='/data/3D_object_detection_tracking(autonomous_driving)'


#sftp connection 을 이용하여 파일 다운로드
#log 테이블 데이터 삽입
def log_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:

            file_path=remote_path + '/' + dir_name + '/meta/log.json'
            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                log_data=json.load(f)

                if ('device_wlh' in log_data):
                    device_width=log_data['device_wlh'][0]
                    device_length=log_data['device_wlh'][1]
                    device_height=log_data['device_wlh'][2]
                else:
                    device_width=None
                    device_length=None
                    device_height=None

                data=(log_data['uuid'], log_data['map_uuids'], log_data['device'], log_data['date_captured'], log_data['location'], log_data['driving_distance'], device_width, device_length, device_height)

                query="INSERT IGNORE INTO log (uuid, map_uuids, device,date_captured, location, driving_distance, device_width, device_length, device_height) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#ego_pose 테이블 데이터 삽입
def ego_pose_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/ego_pose.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                data=[]

                for ego_pose_data in json_data:
                    data.append((ego_pose_data['uuid'],ego_pose_data['timestamp'],
                                 ego_pose_data['translation'][0],ego_pose_data['translation'][1],
                                 ego_pose_data['translation'][2],ego_pose_data['rotation'][0],
                                 ego_pose_data['rotation'][1],ego_pose_data['rotation'][2],
                                 ego_pose_data['rotation'][3],ego_pose_data['coordinate_system']))

                query="INSERT IGNORE INTO ego_pose (uuid, timestamp, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, coordinate_system) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#annotation_type 테이블 데이터 삽입 함수
def annotation_type_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='annotation_type.json'

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
                annotation_type_data=json_data['annotation_type']['contents']
                data=[]

                for annotation_data in annotation_type_data:
                    data.append((annotation_data['name'],annotation_data['description']))

                query="INSERT IGNORE INTO annotation_type (name, description) VALUES (%s, %s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)
    

#sftp connection 을 이용하여 파일 다운로드
#visibility 테이블 데이터 삽입 함수
def visibility_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='visibility.json'

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
                json_data2=json_data['visibility']['contents']
                data=[]

                for visibility_data in json_data2:
                    data.append((visibility_data['level'],visibility_data['description']))

                query="INSERT IGNORE INTO visibility (level, description) VALUES (%s, %s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


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


#sensor 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def sensor_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/sensor.json'
            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data=json.load(f)

                data=[]

                for sensor_data in json_data:

                    uuid=sensor_data['uuid']
                    name=sensor_data['name']
                    type=sensor_data['type']
                    translation_x=sensor_data['translation'][0]
                    translation_y=sensor_data['translation'][1]
                    translation_z=sensor_data['translation'][2]
                    rotation_q0=sensor_data['rotation'][0]
                    rotation_q1=sensor_data['rotation'][1]
                    rotation_q2=sensor_data['rotation'][2]
                    rotation_q3=sensor_data['rotation'][3]
                    height_from_ground=sensor_data['height_from_ground']

                    if sensor_data['intrinsic']['model'] == None:
                    
                        uuid=sensor_data['uuid']
                        intrinsic_model=None
                        intrinsic_parameter_fx=None
                        intrinsic_parameter_skew1=None
                        intrinsic_parameter_cx=None
                        intrinsic_parameter_skew2=None
                        intrinsic_parameter_fy=None
                        intrinsic_parameter_cy=None
                        intrinsic_parameter_projection_vector=None

                        data.append((uuid,name,type,translation_x,translation_y,translation_z,rotation_q0,rotation_q1,rotation_q2,rotation_q3,intrinsic_model,intrinsic_parameter_fx,intrinsic_parameter_skew1,intrinsic_parameter_cx,intrinsic_parameter_skew2,intrinsic_parameter_fy,intrinsic_parameter_cy,intrinsic_parameter_projection_vector,height_from_ground))

                
                    else:
                        uuid=sensor_data['uuid']
                        intrinsic_model=sensor_data['intrinsic']['model']
                        intrinsic_parameter_fx=sensor_data['intrinsic']['parameter'][0][0]
                        intrinsic_parameter_skew1=sensor_data['intrinsic']['parameter'][0][1]
                        intrinsic_parameter_cx=sensor_data['intrinsic']['parameter'][0][2]
                        intrinsic_parameter_skew2=sensor_data['intrinsic']['parameter'][1][0]
                        intrinsic_parameter_fy=sensor_data['intrinsic']['parameter'][1][1]
                        intrinsic_parameter_cy=sensor_data['intrinsic']['parameter'][1][2]
                        intrinsic_parameter_projection_vector=sensor_data['intrinsic']['parameter'][2]

                        data.append((uuid,name,type,translation_x,translation_y,translation_z,rotation_q0,rotation_q1,rotation_q2,rotation_q3,intrinsic_model,intrinsic_parameter_fx,intrinsic_parameter_skew1,intrinsic_parameter_cx,intrinsic_parameter_skew2,intrinsic_parameter_fy,intrinsic_parameter_cy,json.dumps(intrinsic_parameter_projection_vector),height_from_ground))

                query="INSERT IGNORE INTO sensor (uuid, name, type, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, intrinsic_model, intrinsic_parameter_fx, intrinsic_parameter_skew1, intrinsic_parameter_cx, intrinsic_parameter_skew2, intrinsic_parameter_fy, intrinsic_parameter_cy, intrinsic_parameter_projection_vector, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#dataset 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def dataset_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='dataset.json'

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

                data=(json_data['uuid'],json_data['log_uuid'],json_data['name'],json_data['description'],json_data['base_path'],json_data['num_frames'],json_data['first_frame_uuid'],json_data['last_frame_uuid'])

                query="INSERT IGNORE INTO dataset (uuid, log_uuid, name, description, base_path, num_frames, first_frame_uuid, last_frame_uuid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


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


#frame 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def frame_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='frame.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/frame.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)

                data=[]
                for frame_data in json_data:
                    data.append((frame_data['uuid'],frame_data['timestamp'],frame_data['dataset_uuid'],frame_data['prev'],frame_data['next']))
                    
                query="INSERT IGNORE INTO frame (uuid, timestamp, dataset_uuid, prev, next) VALUES (%s,%s,%s,%s,%s)"
                    
                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#frame_data 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def frame_data_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='frame_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/frame_data.json'
            sftp_client.get(file_path, local_path)
            with open(local_path) as f:
                json_data=json.load(f)
                data=[]
                for frame_data_data in json_data:
                    data.append((frame_data_data['uuid'],
                                frame_data_data['frame_uuid'],
                                frame_data_data['ego_pose_uuid'],
                                frame_data_data['sensor_uuid'],
                                frame_data_data['timestamp'],
                                frame_data_data['is_key_frame'],
                                frame_data_data['height'],
                                frame_data_data['width'],
                                frame_data_data['file_name'],
                                frame_data_data['file_format'],
                                frame_data_data['prev'],
                                frame_data_data['next'],
                                frame_data_data['prev_key_frame_data'],
                                frame_data_data['next_key_frame_data']))


                query="INSERT IGNORE INTO frame_data (uuid, frame_uuid, ego_pose_uuid, sensor_uuid, timestamp, is_key_frame, height, width, file_name, file_format, prev, next, prev_key_frame_data, next_key_frame_data) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


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


#scenarios_in_dataset 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def scenarios_in_dataset_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='scenarios_in_dataset.json'

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

                data=[]

                if isinstance(json_data,list):
                    for dataset_data in json_data:
                        dataset_uuid=dataset_data['uuid']
                        scenario_names=dataset_data['scenario_names']

                    for scenario_name in scenario_names:

                        data.append((dataset_uuid,scenario_name))
                else:
                    dataset_uuid=json_data['uuid']
                    scenario_names=json_data['scenario_names']
                    for scenario_name in scenario_names:

                        data.append((dataset_uuid,scenario_name))

                query="INSERT IGNORE INTO scenarios_in_dataset (dataset_uuid,scenario_name) VALUES (%s,%s)"

                cur.executemany(query,data)
                conn.commit()
    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('kakao_mobility_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    log_insert=PythonOperator(task_id='log_insert', python_callable=log_insert)
    ego_pose_insert=PythonOperator(task_id='ego_pose_insert', python_callable=ego_pose_insert)
    annotation_type_insert=PythonOperator(task_id='annotation_type_insert', python_callable=annotation_type_insert)
    visibility_insert=PythonOperator(task_id='visibility_insert', python_callable=visibility_insert)
    category_insert=PythonOperator(task_id='category_insert', python_callable=category_insert)
    scenario_insert=PythonOperator(task_id='scenario_insert', python_callable=scenario_insert)
    sensor_insert=PythonOperator(task_id='sensor_insert', python_callable=sensor_insert)
    dataset_insert=PythonOperator(task_id='dataset_insert', python_callable=dataset_insert)
    instance_insert=PythonOperator(task_id='instance_insert', python_callable=instance_insert)
    frame_insert=PythonOperator(task_id='frame_insert', python_callable=frame_insert)
    frame_data_insert=PythonOperator(task_id='frame_data_insert', python_callable=frame_data_insert)
    frame_annotation_insert=PythonOperator(task_id='frame_annotation_insert', python_callable=frame_annotation_insert)
    scenarios_in_dataset_insert=PythonOperator(task_id='scenarios_in_dataset_insert', python_callable=scenarios_in_dataset_insert)


[log_insert, ego_pose_insert, annotation_type_insert, visibility_insert, category_insert, scenario_insert, sensor_insert, dataset_insert, instance_insert, frame_insert, frame_data_insert, scenarios_in_dataset_insert, frame_annotation_insert]
