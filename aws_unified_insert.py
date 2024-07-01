import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.empty import EmptyOperator
import json
import os
import secrets

default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


kakao_remote_path='/data/3D_object_detection_tracking(autonomous_driving)'



#sftp connection 을 이용하여 파일 다운로드
#kakao annotation_type 테이블 데이터 삽입
def kakao_annotation_type_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_annotation_type.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/preset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
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
    os.remove(kakao_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao attribute 테이블 데이터 삽입
def kakao_attribute_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_attrbute.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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


                            # nuScenes 와 중복 있는 경우 제외
                            if new_name not in ["human.standing", "human.sitting", "human.moving", "human.lying_down", "rider.with_rider", "rider.without_rider"]:
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
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes attribute 테이블 데이터 삽입
def nuScenes_attribute_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_attribute.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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



#sftp connection 을 이용하여 파일 다운로드
#kakao calibrated_sensor 테이블 데이터 삽입
def kakao_calibrated_sensor_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_calibrated_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/sensor.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:

                json_data=json.load(f)
                data=[]

                for calibrated_sensor_data in json_data:

                    uuid=calibrated_sensor_data['uuid']
                    token=uuid.replace("-","")

                    sensor_name=calibrated_sensor_data['name']
                    sql="SELECT token FROM sensor WHERE channel=%s"
                    param=(sensor_name,)
                    cur.execute(sql, param)
                    result=cur.fetchone()

                    sensor_token=result[0]
                    translation=calibrated_sensor_data['translation']
                    rotation=calibrated_sensor_data['rotation']
                    camera_intrinsic=calibrated_sensor_data['intrinsic']
                    height_from_ground=calibrated_sensor_data['height_from_ground']

                    if token=="13826d14e1014ecca56a7730f1c4eec9":
                        print(translation, camera_intrinsic)

                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                    if token=="13826d14e1014ecca56a7730f1c4eec9":
                        print(json.dumps(translation), json.dumps(camera_intrinsic))

                query="INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes calibrated_sensor 테이블 데이터 삽입
def nuScenes_calibrated_sensor_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_calibrated_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/calibrated_sensor.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]
            
            for calibrated_sensor_data in json_data:

                token=calibrated_sensor_data['token']
                sensor_token=calibrated_sensor_data['sensor_token']
                translation=calibrated_sensor_data['translation']
                rotation=calibrated_sensor_data['rotation']
                height_from_ground=calibrated_sensor_data['translation'][2]
                
                if calibrated_sensor_data['camera_intrinsic']:
                    camera_intrinsic=calibrated_sensor_data['camera_intrinsic']

                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                else:
                    camera_intrinsic=None
                    
                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), camera_intrinsic, height_from_ground))
                                

            query="INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"


            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


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
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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

    nuScenes_remote_path='/data/nuScenes_lidarseg/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_category.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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



#sftp connection 을 이용하여 파일 다운로드
#kakao dataset 테이블 데이터 삽입
def kakao_dataset_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                uuid=json_data['uuid']
                token=uuid.replace("-","")

                log_uuid=json_data['log_uuid']
                log_token=log_uuid.replace("-","")

                name=json_data['name']
                num_frames=json_data['num_frames']

                first_frame_uuid=json_data['first_frame_uuid']
                first_frame_token=first_frame_uuid.replace("-","")

                last_frame_uuid=json_data['last_frame_uuid']
                last_frame_token=last_frame_uuid.replace("-","")

                data=(token, log_token, name, num_frames, first_frame_token, last_frame_token)

                query="INSERT IGNORE INTO dataset (token, log_token, name, num_frames, first_frame_token, last_frame_token) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)




#nuScenes dataset 테이블 데이터 삽입
def nuScenes_dataset_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_dataset_scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/scene.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for scene_data in json_data:
                token=scene_data['token']
                log_token=scene_data['log_token']
                num_frames=scene_data['nbr_samples']
                first_frame_token=scene_data['first_sample_token']
                last_frame_token=scene_data['last_sample_token']
                name=scene_data['name']
                
                data.append((token, log_token, num_frames, first_frame_token,last_frame_token, name))
                print(data)

            query="INSERT IGNORE INTO dataset (token, log_token, num_frames, first_frame_token,last_frame_token, name) VALUES (%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao ego_pose 테이블 데이터 삽입
def kakao_ego_pose_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao ego_pose 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/ego_pose.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)
                data=[]

                for ego_pose_data in json_data:

                    uuid=ego_pose_data['uuid']
                    token=uuid.replace("-","")

                    timestamp=ego_pose_data['timestamp']
                    translation=ego_pose_data['translation']
                    rotation=ego_pose_data['rotation']
                    coordinate_system=ego_pose_data['coordinate_system']

                    if coordinate_system==None:
                        data.append((token, timestamp, json.dumps(translation), json.dumps(rotation), coordinate_system))

                    else:
                        data.append((token, timestamp, json.dumps(translation), json.dumps(rotation), json.dumps(coordinate_system)))

                query="INSERT IGNORE INTO ego_pose (token, timestamp, translation, rotation, coordinate_system) VALUES (%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes ego_pose 테이블 데이터 삽입
def nuScenes_ego_pose_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/ego_pose.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for ego_pose_data in json_data:
                token=ego_pose_data['token']
                timestamp=str(ego_pose_data['timestamp'])
                translation=ego_pose_data['translation']
                rotation=ego_pose_data['rotation']
                
                data.append((token, timestamp, json.dumps(translation), json.dumps(rotation)))

            query="INSERT IGNORE INTO ego_pose (token, timestamp, translation, rotation) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao frame_annotation 테이블 데이터 삽입
def kakao_frame_annotation_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_frame_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")


    # attribute 테이블에서 name과 token 가져오기
    query = "SELECT name, token FROM attribute"
    cur.execute(query)

    # 결과를 딕셔너리에 저장
    attribute_data=cur.fetchall()
    attribute_dict = {name: token for name, token in attribute_data}

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
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

                data=[]
                

                print(len(json_data))

                for i in range(len(json_data)):

                    

                    frame_annotation_data = json_data[i]

                    uuid=frame_annotation_data['uuid']
                    token=uuid.replace("-","")

                    frame_data_uuid=frame_annotation_data['frame_data_uuid']
                    frame_data_token=frame_data_uuid.replace("-","")

                    """
                    #kakao 는 frame_data_token 로 맵핑하기때문에 nuScenes 처럼 frame_token 데이터도 넣어주기 위해 frame_data_token 을 이용해서 frame_token 가져옴
                    sql="SELECT frame_token FROM frame_data WHERE token=%s"
                    param=(frame_data_token,)

                    #print("frame_token get start")
                    cur.execute(sql,param)
                    result=cur.fetchone()
                    #print("frame_token get end")

                    frame_token=result[0]
                    """

                    frame_data_token=frame_data_uuid.replace("-","")

                    instance_uuid=frame_annotation_data['instance_uuid']
                    instance_token=instance_uuid.replace("-","")

                    # is_visible 로 visibility_token(kakao에서의 visibility_level) 값을 넣어주기 위해서
                    # visibility 테이블의 token 값을 is_visible 에 맞게 할당함
                    # bbox_pcd3d 인 경우 geometry 에 is_visible 정보가 없기 때문에, visibility_token 정보를 취득할 수 없음. 따라서 null 처리
                    is_visible=frame_annotation_data['geometry'].get('is_visible')

                    if is_visible==None:
                        visibility_token=None

                    elif is_visible is True:
                        visibility_token="5"

                    elif is_visible is False:
                        visibility_token="0"
                    
                    annotation_type_name=frame_annotation_data['annotation_type_name']
                    is_lidar_synced=frame_annotation_data['is_lidar_synced']

                    # wlh, center, orientation, bbox_image3d 는 데이터에 따라서 geometry 데이터가 다르기때문에 값이 있을수도 없을 수도 있음
                    # 따라서 값이 없을때는 json 문자열로 바꿔주는 것이 아니라 None 값으로 놔둬야함
                    size_wlh=frame_annotation_data['geometry'].get('wlh')
                    if size_wlh:
                        size_wlh=json.dumps(size_wlh)

                    translation_center=frame_annotation_data['geometry'].get('center')
                    if translation_center:
                        translation_center=json.dumps(translation_center)

                    rotation_orientation=frame_annotation_data['geometry'].get('orientation')
                    if rotation_orientation:
                        rotation_orientation=json.dumps(rotation_orientation)

                    bbox_image3d=frame_annotation_data['geometry'].get('bbox_image3d')
                    if bbox_image3d:
                        bbox_image3d=json.dumps(bbox_image3d)

                    num_lidar_pts=frame_annotation_data['num_pts']


                    #attribute 의 데이터를 가공해서 name 을 만들어 준 후 name 에 해당하는 token 을 가져와서 배열에 넣어줌. attribute_tokens 이 attribute 에 해당하는 토큰 배열이므로 그 형태 만들어줌

                    if frame_annotation_data['attribute'] is not None:

                        # 데이터 한 행마다 attribute_tokens 배열은 초기화 되어야함
                        # 한 instance(= annotation 하나) 가 가지고있는 attribute 에 대한 데이터이기 때문
                        result_attribute_token=[]

                        for key, value in frame_annotation_data['attribute'].items():
                            # '_state'가 키에 포함되어 있는지 확인
                            if '_state' in key:
                                # '_state'를 제거
                                new_key = key.replace('_state', '')
                            else:
                                # '_state'가 없으면 그대로 사용
                                new_key = key
                            
                            # 키와 값을 연결하여 새로운 문자열 생성
                            new_name = f"{new_key}.{value}"

                            if new_name == "human.standing":
                                new_name="pedestrian.standing"

                            elif new_name == "human.moving":
                                new_name="pedestrian.moving"

                            elif new_name == "human.sitting" or new_name == "human.lying_down":
                                new_name="pedestrian.sitting_lying_down"
                            
                            elif new_name == "rider.with_rider":
                                new_name="cycle.with_rider"

                            elif new_name == "rider.without_rider":
                                new_name="cycle.without_rider"

                            attribute_token=attribute_dict.get(new_name)
                            result_attribute_token.append(attribute_token)

                        result_attribute_token=json.dumps(result_attribute_token)

                    else:
                        result_attribute_token = None

                    
                    frame_aligned=frame_annotation_data['frame_aligned']
                    description=frame_annotation_data['description']

                    if frame_annotation_data['prev']:
                        prev=frame_annotation_data['prev'].replace("-","")
                    else:
                        prev=None

                    if frame_annotation_data['next']:
                        next=frame_annotation_data['next'].replace("-","")
                    else:
                        next=None


                    # 처음 데이터 넣어줄때는 frame_data_token 으로 frame_token select 하면 오래걸리기 때문에 frame_token 은 넣어주지않음
                    #모든 데이터 넣어준 후에 temp 테이블을 이용해서 한번에 insert 해줌
                    data.append((token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, result_attribute_token, frame_aligned, description, prev, next))
                    #print("data append")

                    #50000개 단위로 insert
                    if (i % 50000 == 0 and i>0):

                        print("50000 data append end")

                        query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                        print("%d executemany start" %(i//50000))
                        cur.executemany(query,data)
                        print("%d executemany end" %(i//50000))

                        data = []

                # 나머지 데이터 삽입 ( 마지막 50000개까지 안 채워진 데이터 )
                query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                print("last executemany start")
                cur.executemany(query,data)
                print("last executemany end")

                #commit은 트랜잭션 고려해서 한 데이터셋에 대한 모든 데이터가 insert 됐을 때 마지막에 실행
                print("commit start")
                conn.commit()
                print("insert end")
        

        #최종 frame_annotation 테이블와 동일한 temp 테이블을 만듦
        query="CREATE TEMPORARY TABLE IF NOT EXISTS temp SELECT fa.token, fd.frame_token, fa.frame_data_token,fa.instance_token, fa.visibility_token,fa.annotation_type_name, fa.is_lidar_synced, fa.size_wlh, fa.translation_center, fa.rotation_orientation, fa.bbox_image3d, fa.num_lidar_pts, fa.attribute_tokens, fa.frame_aligned, fa.description, fa.prev, fa.`next` FROM frame_annotation fa JOIN frame_data fd ON fa.frame_data_token=fd.token"

        cur.execute(query)

        query="DELETE FROM frame_annotation"
        cur.execute(query)
        conn.commit()

        #temp 테이블을 그대로 frame_annotation 에 넣어줌
        query="INSERT INTO frame_annotation(token, frame_token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) SELECT * FROM temp"

        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes frame_annotation 테이블 데이터 삽입
def nuScenes_frame_annotation_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_frame_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sample_annotation.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for sample_annotation_data in json_data:

                token=sample_annotation_data['token']
                frame_token=sample_annotation_data['sample_token']
                instance_token=sample_annotation_data['instance_token']
                visibility_token=sample_annotation_data['visibility_token']
                size_wlh=sample_annotation_data['size']
                translation_center=sample_annotation_data['translation']
                rotation_orientation=sample_annotation_data['rotation']
                num_lidar_pts=sample_annotation_data['num_lidar_pts']
                num_radar_pts=sample_annotation_data['num_radar_pts']

                # attribute_tokens 가 [] 빈배열 값일때, null 처리
                if sample_annotation_data['attribute_tokens']:
                    attribute_tokens=json.dumps(sample_annotation_data['attribute_tokens'])
                else:
                    attribute_tokens=None

                if sample_annotation_data['prev']=="":
                    prev=None
                else:
                    prev=sample_annotation_data['prev'].replace("-","")
                    

                if sample_annotation_data['next']=="":
                    next=None
                else:
                    next=sample_annotation_data['next'].replace("-","")

                
                data.append((token, frame_token, instance_token, visibility_token, json.dumps(size_wlh), json.dumps(translation_center), json.dumps(rotation_orientation), num_lidar_pts, num_radar_pts, attribute_tokens, prev, next))

            query="INSERT IGNORE INTO frame_annotation (token, frame_token, instance_token, visibility_token, size_wlh, translation_center, rotation_orientation, num_lidar_pts, num_radar_pts, attribute_tokens, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao frame_data 테이블 데이터 삽입
def kakao_frame_data_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_frame_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/frame_data.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                for frame_data_data in json_data:

                    uuid=frame_data_data['uuid']
                    token=uuid.replace("-","")

                    frame_uuid=frame_data_data['frame_uuid']
                    frame_token=frame_uuid.replace("-","")

                    ego_pose_uuid=frame_data_data['ego_pose_uuid']
                    ego_pose_token=ego_pose_uuid.replace("-","")

                    sensor_uuid=frame_data_data['sensor_uuid']
                    calibrated_sensor_token=sensor_uuid.replace("-","")

                    timestamp=frame_data_data['timestamp']
                    is_key_frame=frame_data_data['is_key_frame']

                    if frame_data_data['height']==None:
                        height=0
                    else:
                        height=frame_data_data['height']

                    if frame_data_data['width']==None:
                        width=0
                    else:
                        width=frame_data_data['width']
                    
                    file_name=frame_data_data['file_name']
                    file_format=frame_data_data['file_format']

                    if frame_data_data['prev']:
                        prev=frame_data_data['prev'].replace("-","")
                    else:
                        prev=None

                    if frame_data_data['next']:
                        next=frame_data_data['next'].replace("-","")
                    else:
                        next=None

                    data.append((token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next))


                query="INSERT IGNORE INTO frame_data (token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes frame_data 테이블 데이터 삽입
def nuScenes_frame_data_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_frame_data_sample_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sample_data.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for sample_data_data in json_data:

                token=sample_data_data['token']
                frame_token=sample_data_data['sample_token']
                ego_pose_token=sample_data_data['ego_pose_token']
                calibrated_sensor_token=sample_data_data['calibrated_sensor_token']
                timestamp=str(sample_data_data['timestamp'])
                is_key_frame=sample_data_data['is_key_frame']
                height=sample_data_data['height']
                width=sample_data_data['width']
                file_name=sample_data_data['filename']
                file_format=sample_data_data['fileformat']

                if sample_data_data['prev']=="":
                    prev=None
                else:
                    prev=sample_data_data['prev']

                if sample_data_data['next']=="":
                    next=None
                else:
                    next=sample_data_data['next']


                data.append((token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next))

            query="INSERT IGNORE INTO frame_data (token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao frame 테이블 데이터 삽입
def kakao_frame_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_frame.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao scenario 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/frame.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                for frame_data in json_data:

                    uuid=frame_data['uuid']
                    token=uuid.replace("-","")

                    timestamp=frame_data['timestamp']
                    
                    dataset_uuid=frame_data['dataset_uuid']
                    dataset_token=dataset_uuid.replace("-","")

                    prev=frame_data['prev']
                    if prev!=None:
                        prev=prev.replace("-","")

                    next=frame_data['next']
                    if next!=None:
                        next=next.replace("-","")


                    data.append((token, timestamp, dataset_token, prev, next))
                    
                query="INSERT IGNORE INTO frame (token, timestamp, dataset_token, prev, next) VALUES (%s,%s,%s,%s,%s)"
                    
                cur.executemany(query,data)
                conn.commit()


    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes frame 테이블 데이터 삽입
def nuScenes_frame_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuSceens_frame_sample.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sample.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for sample_data in json_data:
                token=sample_data['token']
                timestamp=str(sample_data['timestamp'])

                prev=sample_data['prev']
                if prev=="":
                    prev=None

                next=sample_data['next']
                if next=="":
                    next=None
                
                dataset_token=sample_data['scene_token']
                
                data.append((token, timestamp, dataset_token, prev, next))

            query="INSERT IGNORE INTO frame (token, timestamp, dataset_token, prev, next) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao instance 테이블 데이터 삽입
def kakao_instance_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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



#nuScenes lidarseg 테이블 데이터 삽입
def nuScenes_lidarseg_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes_lidarseg/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_lidarseg_map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/lidarseg.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]

            for lidarseg_data in json_data:

                token=lidarseg_data['token']
                frame_data_token=lidarseg_data['sample_data_token']
                filename=lidarseg_data['filename']
                
                data.append((token, frame_data_token, filename))

            query="INSERT IGNORE INTO lidarseg (token, frame_data_token, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao log 테이블 데이터 삽입
def kakao_log_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao ego_pose 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/log.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:

                log_data=json.load(f)

                uuid=log_data['uuid']
                token=uuid.replace("-","")

                device=log_data['device']

                date_captured=log_data['date_captured']

                if log_data['location']=="":
                    location=None
                else:
                    location=log_data['location']

                driving_distance=log_data['driving_distance']

                device_wlh=log_data.get('device_wlh')

                if device_wlh is not None:
                    device_wlh=json.dumps(device_wlh)
    

                data=(token, device, date_captured, location, driving_distance, device_wlh)

                query="INSERT IGNORE INTO log (token, device, date_captured, location, driving_distance, device_wlh) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes log 테이블 데이터 삽입
def nuScenes_log_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/log.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for log_data in json_data:
                token=log_data['token']
                device=log_data['vehicle']
                date_captured=log_data['date_captured']
                location=log_data['location']
                logfile=log_data['logfile']
                
                
                data.append((token, device, date_captured, location, logfile))

            query="INSERT IGNORE INTO log (token, device, date_captured, location, logfile) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


#sftp connection 을 이용하여 파일 다운로드
#kakao log_map 테이블 데이터 삽입
def kakao_log_map_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_log_map_log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_log_map_map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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



#nuScenes map 테이블 데이터 삽입
def nuScenes_map_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_map_map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/map.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for map_data in json_data:

                token=map_data['token']
                category=map_data['category']
                filename=map_data['filename']
                
                data.append((token, category, filename))

            query="INSERT IGNORE INTO map (token, category, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#scenario 테이블 데이터 삽입
def kakao_scenario_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_scenario_dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao scenario 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)
                json_data2=json_data['scenario_names']

                data=[]
                for scenario_data_name in json_data2:
                    data.append(scenario_data_name)

                query="INSERT IGNORE INTO scenario (description) VALUES (%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes scenario 테이블 데이터 삽입
def nuScenes_scenario_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_scenario_scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_local_path='scene.json'
        nuScenes_file_path=nuScenes_remote_path + '/scene.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for scene_data in json_data:
                scene_description=scene_data['description']

                #scene 에 대한 n개의 설명이 하나의 문자열로 저장되어있는 description 을 ','로 구분해서 개별 데이터로 넣어줌
                data.extend((item.strip(),) for item in scene_description.split(','))

            query="INSERT IGNORE INTO scenario (description) VALUES (%s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao scenarios_in_dataset 테이블 데이터 삽입
def kakao_scenarios_in_dataset_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_scenarios_in_dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao scenario 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/dataset.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            with open(kakao_local_path) as f:
                json_data=json.load(f)

                data=[]

                #dataset이 여러개인 경우 (json 배열)을 위해
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

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_scenarios_in_dataset.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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



#sftp connection 을 이용하여 파일 다운로드
#kakao sensor 테이블 데이터 삽입
def kakao_sensor_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/sensor.json'
            sftp_client.get(kakao_file_path, kakao_local_path)

            # 이미 존재하는 channel = name 목록 가져오기
            cur.execute("SELECT channel FROM sensor")
            existing_names = {row[0] for row in cur.fetchall()}

            with open(kakao_local_path) as f:

                json_data=json.load(f)
                data=[]

                for sensor_data in json_data:

                    channel=sensor_data['name']
                    modality=sensor_data['type']
                    
                    data.append((channel,modality))


                # 중복 데이터 필터링
                filtered_data = []

                #중복된 name (camero(00)....)이 존재하지 않는 경우에만 token 생성후 데이터 삽입
                for entry in data:

                    channel, modality = entry[0], entry[1]

                    if channel not in existing_names:
                        token = secrets.token_hex(16)
                        filtered_data.append((token, channel, modality))
                
                query="INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"
                
                cur.executemany(query, filtered_data)
                conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)



#nuScenes sensor 테이블 데이터 삽입
def nuScenes_sensor_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sensor.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:

            json_data=json.load(f)
            data=[]
            
            for sensor_data in json_data:
        
                token=sensor_data['token']
                channel=sensor_data['channel']
                modality=sensor_data['modality']

                data.append((token, channel, modality))

            query="INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)



#sftp connection 을 이용하여 파일 다운로드
#kakao visibility 테이블 데이터 삽입
def kakao_visibility_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_visibility.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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

    nuScenes_remote_path='/data/nuScenes/v1.0-mini'
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_visibility.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='aws_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use adds")

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


with DAG('aws_unified_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_annotation_type_insert=PythonOperator(task_id='kakao_annotation_type_insert', python_callable=kakao_annotation_type_insert)
    kakao_attribute_insert=PythonOperator(task_id='kakao_attribute_insert', python_callable=kakao_attribute_insert)
    nuScenes_attribute_insert=PythonOperator(task_id='nuScenes_attribute_insert', python_callable=nuScenes_attribute_insert)
    kakao_calibrated_sensor_insert=PythonOperator(task_id='kakao_calibrated_sensor_insert', python_callable=kakao_calibrated_sensor_insert)
    nuScenes_calibrated_sensor_insert=PythonOperator(task_id='nuScenes_calibrated_sensor_insert', python_callable=nuScenes_calibrated_sensor_insert)
    kakao_category_insert=PythonOperator(task_id='kakao_category_insert', python_callable=kakao_category_insert)
    nuScenes_category_insert=PythonOperator(task_id='nuScenes_category_insert', python_callable=nuScenes_category_insert)
    kakao_dataset_insert=PythonOperator(task_id='kakao_dataset_insert', python_callable=kakao_dataset_insert)
    nuScenes_dataset_insert=PythonOperator(task_id='nuScenes_dataset_insert', python_callable=nuScenes_dataset_insert)
    kakao_ego_pose_insert=PythonOperator(task_id='kakao_ego_pose_insert', python_callable=kakao_ego_pose_insert)
    nuScenes_ego_pose_insert=PythonOperator(task_id='nuScenes_ego_pose_insert', python_callable=nuScenes_ego_pose_insert)
    kakao_frame_annotation_insert=PythonOperator(task_id='kakao_frame_annotation_insert', python_callable=kakao_frame_annotation_insert)
    nuScenes_frame_annotation_insert=PythonOperator(task_id='nuScenes_frame_annotation_insert', python_callable=nuScenes_frame_annotation_insert)
    kakao_frame_data_insert=PythonOperator(task_id='kakao_frame_data_insert', python_callable=kakao_frame_data_insert)
    nuScenes_frame_data_insert=PythonOperator(task_id='nuScenes_frame_data_insert', python_callable=nuScenes_frame_data_insert)
    kakao_frame_insert=PythonOperator(task_id='kakao_frame_insert', python_callable=kakao_frame_insert)
    nuScenes_frame_insert=PythonOperator(task_id='nuScenes_frame_insert', python_callable=nuScenes_frame_insert)
    kakao_instance_insert=PythonOperator(task_id='kakao_instance_insert', python_callable=kakao_instance_insert)
    nuScenes_instance_insert=PythonOperator(task_id='nuScenes_instance_insert', python_callable=nuScenes_instance_insert)
    nuScenes_lidarseg_insert=PythonOperator(task_id='nuScenes_lidarseg_insert', python_callable=nuScenes_lidarseg_insert)
    kakao_log_insert=PythonOperator(task_id='kakao_log_insert', python_callable=kakao_log_insert)
    nuScenes_log_insert=PythonOperator(task_id='nuScenes_log_insert', python_callable=nuScenes_log_insert)
    kakao_log_map_insert=PythonOperator(task_id='kakao_log_map_insert', python_callable=kakao_log_map_insert)
    nuScenes_log_map_insert=PythonOperator(task_id='nuScenes_log_map_insert', python_callable=nuScenes_log_map_insert)
    nuScenes_map_insert=PythonOperator(task_id='nuScenes_map_insert', python_callable=nuScenes_map_insert)
    kakao_scenario_insert=PythonOperator(task_id='kakao_scenario_insert', python_callable=kakao_scenario_insert)
    nuScenes_scenario_insert=PythonOperator(task_id='nuScenes_scenario_insert', python_callable=nuScenes_scenario_insert)
    kakao_scenarios_in_dataset_insert=PythonOperator(task_id='kakao_scenarios_in_dataset_insert', python_callable=kakao_scenarios_in_dataset_insert)
    nuScenes_scenarios_in_dataset_insert=PythonOperator(task_id='nuScenes_scenarios_in_dataset_insert', python_callable=nuScenes_scenarios_in_dataset_insert)
    kakao_sensor_insert=PythonOperator(task_id='kakao_sensor_insert', python_callable=kakao_sensor_insert)
    nuScenes_sensor_insert=PythonOperator(task_id='nuScenes_sensor_insert', python_callable=nuScenes_sensor_insert)
    kakao_visibility_insert=PythonOperator(task_id='kakao_visibility_insert', python_callable=kakao_visibility_insert)
    nuScenes_visibility_insert=PythonOperator(task_id='nuScenes_visibility_insert', python_callable=nuScenes_visibility_insert)


# nuScenes 태스크들을 그룹화
nuScenes_tasks = [
    nuScenes_attribute_insert, nuScenes_calibrated_sensor_insert, nuScenes_category_insert,
    nuScenes_dataset_insert, nuScenes_ego_pose_insert, nuScenes_frame_data_insert,
    nuScenes_frame_insert, nuScenes_instance_insert, nuScenes_lidarseg_insert,
    nuScenes_log_insert, nuScenes_log_map_insert, nuScenes_map_insert,
    nuScenes_scenario_insert, nuScenes_scenarios_in_dataset_insert,
    nuScenes_sensor_insert, nuScenes_visibility_insert
]

# kakao 태스크들을 그룹화
kakao_tasks = [
    kakao_annotation_type_insert, kakao_attribute_insert,
    kakao_category_insert, kakao_dataset_insert, kakao_ego_pose_insert,
    kakao_frame_data_insert, kakao_frame_insert, kakao_log_insert, kakao_log_map_insert,
    kakao_scenario_insert, kakao_scenarios_in_dataset_insert, kakao_sensor_insert,
    kakao_visibility_insert
]

# 더미 태스크를 생성하여 그룹 간 종속성을 설정
nuScenes_end = EmptyOperator(task_id='nuScenes_end', dag=dag)
kakao_start = EmptyOperator(task_id='kakao_start', dag=dag)
kakao_end = EmptyOperator(task_id='kakao_end', dag=dag)

# nuScenes 태스크들을 nuScenes_end에 연결
for task in nuScenes_tasks:
    task >> nuScenes_end

# nuScenes_end를 kakao_start에 연결
nuScenes_end >> kakao_start

# kakao_start를 kakao 태스크들에 연결
for task in kakao_tasks:
    kakao_start >> task >> kakao_end

# 나머지 태스크들의 종속성 설정
kakao_end >> kakao_calibrated_sensor_insert >> kakao_instance_insert >> kakao_frame_annotation_insert >> nuScenes_frame_annotation_insert

