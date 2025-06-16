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
from airflow.decorators import task
import pymysql
import shutil
import yaml
from utils.yaml_no_bool import load_yaml     # 새 유틸 임포트

default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes = 5),
}

kakao_remote_path = '/data/tail_light_detection(autonomous_vehicle)/Tail_Light'

def prepare_local_dir(dir_name):
    local_dir = f"tail_light_detection(autonomous_vehicle)/{dir_name}"
    os.makedirs(local_dir, exist_ok=True)
    return local_dir


def cleanup_dirs():
    base_path = "tail_light_detection(autonomous_vehicle)"
    if os.path.exists(base_path):
        shutil.rmtree(base_path)


def annotation_type_insert():
    # SFTP 접속 정보
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    # MySQL 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("USE avtail")

    with hook.get_conn() as sftp_client:
        for dir_name in kakao_directory_list:
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/preset.yaml"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/preset.yaml"

            sftp_client.get(file_path, local_path)

            with open(local_path, 'r', encoding='utf-8') as f:
                yaml_data = yaml.safe_load(f)
                annotation_type_data = yaml_data['annotation_type']['contents']
                data = [
                    (item['name'], item['description'])
                    for item in annotation_type_data
                ]

                query = "INSERT IGNORE INTO annotation_type (name, description) VALUES (%s, %s)"
                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()


def attribute_insert():
    # SFTP 접속 정보
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    # MySQL 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # 사용할 DB 선택
    cur.execute("USE adds")

    # 기존 DB attribute.name 전체 조회
    cur.execute("SELECT name FROM attribute")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB의 attribute ::::: {existing_names}")

    # 최종 insert 대상 attribute name 저장용
    name_list = set()

    # SFTP로 preset.yaml 파일 다운로드 및 파싱
    with hook.get_conn() as sftp_client:
        for dir_name in kakao_directory_list:
            local_dir = prepare_local_dir(dir_name)

            # === [1차] preset.yaml 처리 ===
            preset_remote = f"{kakao_remote_path}/{dir_name}/meta/preset.yaml"
            preset_local = f"{local_dir}/preset.yaml"
            try:
                sftp_client.get(preset_remote, preset_local)
                yaml_data = load_yaml(preset_local)

                for attr_group in yaml_data.get("attribute", {}).get("contents", []):
                    attr_key = attr_group.get("value")  # 예: vehicle_state 등
                    base_key = attr_key.replace("_state", "") if "_state" in attr_key else attr_key

                    for item in attr_group.get("list", []):
                        val = str(item.get("value"))
                        attr_name = f"{base_key}.{val}"

                        # 통일된 이름으로 변환
                        if attr_name == "human.standing":
                            attr_name = "pedestrian.standing"
                        elif attr_name == "human.moving":
                            attr_name = "pedestrian.moving"
                        elif attr_name in ["human.sitting", "human.lying_down"]:
                            attr_name = "pedestrian.sitting_lying_down"
                        elif attr_name == "rider.with_rider":
                            attr_name = "cycle.with_rider"
                        elif attr_name == "rider.without_rider":
                            attr_name = "cycle.without_rider"

                        if attr_name not in existing_names:
                            name_list.add(attr_name)
                            existing_names.add(attr_name)

            except Exception as e:
                print(f"[경고] preset.yaml 누락 or 실패: {preset_remote} / {e}")
            if os.path.exists(preset_local):
                os.remove(preset_local)

            # === [2차] frame_annotation.json 처리 ===
            frame_remote = f"{kakao_remote_path}/{dir_name}/meta/frame_annotation.json"
            frame_local = f"{local_dir}/frame_annotation.json"
            try:
                sftp_client.get(frame_remote, frame_local)
                with open(frame_local, 'r', encoding='utf-8') as f:
                    frame_data = json.load(f)

                for ann in frame_data:
                    attr_dict = ann.get("attribute", {})
                    for key, val in attr_dict.items():
                        base_key = key.replace("_state", "") if "_state" in key else key
                        attr_name = f"{base_key}.{val}"

                        # 통일된 이름으로 변환
                        if attr_name == "human.standing":
                            attr_name = "pedestrian.standing"
                        elif attr_name == "human.moving":
                            attr_name = "pedestrian.moving"
                        elif attr_name in ["human.sitting", "human.lying_down"]:
                            attr_name = "pedestrian.sitting_lying_down"
                        elif attr_name == "rider.with_rider":
                            attr_name = "cycle.with_rider"
                        elif attr_name == "rider.without_rider":
                            attr_name = "cycle.without_rider"

                        if attr_name not in existing_names:
                            name_list.add(attr_name)
                            existing_names.add(attr_name)

            except Exception as e:
                print(f"[경고] frame_annotation.json 누락 or 실패: {frame_remote} / {e}")
            if os.path.exists(frame_local):
                os.remove(frame_local)

    print(f"기존 attribute 테이블에 없는 데이터 ::::: {name_list}")

    # INSERT할 최종 데이터 생성
    data = [(secrets.token_hex(16), name) for name in name_list]

    insert_query = "INSERT IGNORE INTO attribute (token, name) VALUES (%s, %s)"
    cur.executemany(insert_query, data)
    conn.commit()

    cur.close()
    conn.close()


def sensor_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/sensor.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/sensor.json"

            sftp_client.get(file_path, local_path)

            # 이미 존재하는 channel = name 목록 가져오기
            cur.execute("SELECT channel FROM sensor")
            existing_names = {row[0] for row in cur.fetchall()}

            with open(local_path) as f:

                json_data = json.load(f)
                data = []

                for sensor_data in json_data:

                    channel = sensor_data['name']
                    modality = sensor_data['type']
                    
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

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def calibrated_sensor_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)
    
    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    
    cur.execute("USE avtail")

    # sensor 테이블 channel-token 맵핑 (adds DB 기준)
    cur.execute("SELECT channel, token FROM sensor")
    sensor_dict = {row[0]: row[1] for row in cur.fetchall()}

    with hook.get_conn() as sftp_client:

        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:
            
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/calibrated_sensor.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/sensor.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:

                json_data = json.load(f)
                data = []

                for calibrated_sensor_data in json_data:

                    uuid = calibrated_sensor_data['uuid']
                    token = uuid.replace("-","")

                    sensor_name = calibrated_sensor_data['name']
                    sensor_token = sensor_dict.get(sensor_name)

                    if not sensor_token:
                        print(f"[WARN] sensor channel {sensor_name} 에 해당하는 token 없음, 건너뜀")

                    translation = calibrated_sensor_data['translation']
                    rotation = calibrated_sensor_data['rotation']
                    camera_intrinsic = calibrated_sensor_data['intrinsic']
                    height_from_ground = calibrated_sensor_data['height_from_ground']


                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                query = "INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()
            
            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def dataset_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:
            
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/dataset.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/dataset.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data = json.load(f)

                uuid = json_data['uuid']
                token = uuid.replace("-","")
                
                type = 'kakao_mobility'

                log_uuid = json_data['log_uuid']
                log_token = log_uuid.replace("-","")

                base_path = f"tail_light_detection(autonomous_vehicle)/Tail_Light/{json_data['name']}/sensor/"

                num_frames = json_data['num_frames']

                first_frame_uuid = json_data['first_frame_uuid']
                first_frame_token = first_frame_uuid.replace("-","")

                last_frame_uuid = json_data['last_frame_uuid']
                last_frame_token = last_frame_uuid.replace("-","")

                data = (token, type, log_token, base_path, num_frames, first_frame_token, last_frame_token)

                query = "INSERT IGNORE INTO dataset (token, type, log_token, base_path, num_frames, first_frame_token, last_frame_token) VALUES (%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()


def frame_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/frame.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/frame.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data = json.load(f)

                data = []

                for frame_data in json_data:

                    uuid = frame_data['uuid']
                    token = uuid.replace("-","")

                    timestamp = frame_data['timestamp']
                    
                    dataset_uuid = frame_data['dataset_uuid']
                    dataset_token = dataset_uuid.replace("-","")

                    prev = frame_data['prev']
                    if prev != None:
                        prev = prev.replace("-","")

                    next = frame_data['next']
                    if next != None:
                        next = next.replace("-","")


                    data.append((token, timestamp, dataset_token, prev, next))
                    
                query = "INSERT IGNORE INTO frame (token, timestamp, dataset_token, prev, next) VALUES (%s,%s,%s,%s,%s)"
                    
                cur.executemany(query,data)
                conn.commit()

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def ego_pose_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    print(kakao_directory_list)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/ego_pose.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/ego_pose.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data = json.load(f)
                data = []

                for ego_pose_data in json_data:

                    uuid = ego_pose_data['uuid']
                    token = uuid.replace("-","")

                    timestamp = ego_pose_data['timestamp']
                    translation = ego_pose_data['translation']
                    rotation = ego_pose_data['rotation']
                    coordinate_system = ego_pose_data['coordinate_system']
                    pose_type = "vehicle"

                    if coordinate_system == None:
                        data.append((token, timestamp, json.dumps(translation), json.dumps(rotation), coordinate_system, pose_type))

                    else:
                        data.append((token, timestamp, json.dumps(translation), json.dumps(rotation), json.dumps(coordinate_system), pose_type))

                query = "INSERT IGNORE INTO ego_pose (token, timestamp, translation, rotation, coordinate_system, pose_type) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()
            
            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def frame_data_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    # 원래 kakao_mobility insert 코드랑 달라진 부분. 이유: file_name을 nuScenes 처럼 수정해주기 위해서. (경로 추가해줌.)
    query = "SELECT cs.token, s.channel FROM calibrated_sensor cs JOIN sensor s ON cs.sensor_token = s.token"
    cur.execute(query)
    combined_sensor_dict = {row[0]: row[1] for row in cur.fetchall()}

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/frame_data.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/frame_data.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data = json.load(f)

                data = []

                for frame_data_data in json_data:

                    uuid = frame_data_data['uuid']
                    token = uuid.replace("-","")

                    frame_uuid = frame_data_data['frame_uuid']
                    frame_token = frame_uuid.replace("-","")

                    ego_pose_uuid = frame_data_data['ego_pose_uuid']
                    ego_pose_token = ego_pose_uuid.replace("-","")

                    calibrated_sensor_uuid = frame_data_data['sensor_uuid']
                    calibrated_sensor_token = calibrated_sensor_uuid.replace("-","")

                    timestamp = frame_data_data['timestamp']
                    is_key_frame = frame_data_data['is_key_frame']

                    if frame_data_data['height'] == None:
                        height = 0
                    else:
                        height = frame_data_data['height']

                    if frame_data_data['width'] == None:
                        width = 0
                    else:
                        width = frame_data_data['width']
                    
                    file_name = f"{combined_sensor_dict[calibrated_sensor_token]}/{frame_data_data['file_name']}.{frame_data_data['file_format']}"

                    file_format = frame_data_data['file_format']

                    if frame_data_data['prev']:
                        prev = frame_data_data['prev'].replace("-","")
                    else:
                        prev = None

                    if frame_data_data['next']:
                        next = frame_data_data['next'].replace("-","")
                    else:
                        next = None

                    data.append((token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next))


                query = "INSERT IGNORE INTO frame_data (token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame, height, width, file_name, file_format, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def instance_insert():

    # SFTP 및 MySQL 접속
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # adds DB에서 category 조회
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM category")
    category_dict = {row[0]: row[1] for row in cur.fetchall()}
    print(f"기존 DB의 category 세팅 :::::: {category_dict}")

    # av3d로 작업용 DB 변경
    cur.execute("USE av3d")

    # 누락된 category → 나중에 추가할 리스트 (instance.json에서 발견한 새로운 category)
    new_category_list = []

    # instance insert용 누적 리스트
    instance_data_list = []

    with hook.get_conn() as sftp_client:
        for dir_name in kakao_directory_list:
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/instance.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/instance.json"

            sftp_client.get(file_path, local_path)

            with open(local_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)

                for instance_data in json_data:
                    uuid = instance_data['uuid']
                    token = uuid.replace("-", "")

                    category_name = instance_data['category_name']

                    # category_dict에서 바로 확인
                    if category_name in category_dict:
                        category_token = category_dict[category_name]
                    else:
                        # 새로 발견된 category_name
                        category_token = secrets.token_hex(16)
                        category_dict[category_name] = category_token
                        new_category_list.append((category_token, category_name))

                    first_token = instance_data['first_frame_annotation_uuid'].replace("-", "")
                    last_token = instance_data['last_frame_annotation_uuid'].replace("-", "")
                    num_annotations = instance_data['num_annotations']

                    instance_data_list.append(
                        (token, category_token, first_token, last_token, num_annotations)
                    )

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    print(f"새로 발견된 category ::::::: {new_category_list}")

    # instance 테이블 insert
    cur.executemany(
        "INSERT IGNORE INTO instance (token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations) VALUES (%s, %s, %s, %s, %s)",
        instance_data_list
    )
    conn.commit()

    # adds.category 테이블에 새 카테고리 추가
    if new_category_list:
        cur.execute("USE adds")
        cur.executemany(
            "INSERT IGNORE INTO category (token, name) VALUES (%s, %s)",
            new_category_list
        )
        conn.commit()

    # 연결 종료
    cur.close()
    conn.close()


def log_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/log.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/log.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:

                log_data = json.load(f)

                uuid = log_data['uuid']
                token = uuid.replace("-","")

                device = log_data['device']

                date_captured = log_data['date_captured']

                if log_data['location'] == "":
                    location = None
                else:
                    location = log_data['location']

                driving_distance = log_data['driving_distance']

                device_wlh = log_data.get('device_wlh')

                if device_wlh is not None:
                    device_wlh = json.dumps(device_wlh)
    

                data = (token, device, date_captured, location, driving_distance, device_wlh)

                query = "INSERT IGNORE INTO log (token, device, date_captured, location, driving_distance, device_wlh) VALUES (%s,%s,%s,%s,%s,%s)"

                cur.execute(query, data)
                conn.commit()

    cur.close()
    conn.close()


def log_map_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/log_map_log.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/log.json"
            
            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data = json.load(f)

                data = []

                log_uuid = json_data['uuid']
                log_token = log_uuid.replace("-","")

                map_uuids = json_data['map_uuids']

                #log 테이블의 map_uuids 가 null 값이 아닌 배열인 경우에만 삽입
                if map_uuids != None:
                    for map_uuid in map_uuids:
                        #uuid 형식을 token 형식으로 변환해주어서 저장해야함
                        map_token = map_uuid.replace("-","")
                        data.append((log_token, map_token))
     

                    query = "INSERT IGNORE INTO log_map (log_token, map_token) VALUES (%s,%s)"

                    cur.executemany(query,data)
                    conn.commit()

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    cur.close()
    conn.close()


def scenario_insert():

    # SFTP 및 MySQL 접속
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # adds DB에서 기존 scenario description → token 맵 조회
    cur.execute("USE adds")
    cur.execute("SELECT description, token FROM scenario")
    existing_scenarios = {desc: token for desc, token in cur.fetchall()}

    # 다시 avtail로 전환
    cur.execute("USE avtail")

    # insert 대상 누적 리스트
    new_scenario_data = []  # adds DB에 새로 insert할 scenario들
    scenarios_in_dataset_data = []

    # 중복 방지를 위한 local 캐시
    local_description_to_token = existing_scenarios.copy()

    with hook.get_conn() as sftp_client:
        for dir_name in kakao_directory_list:
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/scenario_dataset.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/dataset.json"

            try:
                sftp_client.get(file_path, local_path)
            except Exception as e:
                print(f"[경고] dataset.json 다운로드 실패: {file_path} / {e}")
                continue

            with open(local_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)

                # description 생성
                description = ", ".join(json_data.get("scenario_names", []))

                # dataset_token 준비
                dataset_uuid = json_data.get("uuid", "")
                dataset_token = dataset_uuid.replace("-", "")

                # token 결정: 기존에 있는지 확인
                if description in local_description_to_token:
                    scenario_token = local_description_to_token[description]
                else:
                    scenario_token = secrets.token_hex(16)
                    local_description_to_token[description] = scenario_token
                    new_scenario_data.append((scenario_token, description))

                # 연결 테이블(scenarios_in_dataset)용 데이터
                scenarios_in_dataset_data.append((dataset_token, scenario_token))

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)

    # adds DB에 scenario 추가
    if new_scenario_data:
        cur.execute("USE adds")
        cur.executemany(
            "INSERT IGNORE INTO scenario (token, description) VALUES (%s, %s)",
            new_scenario_data
        )

    # avtail에 연결 정보 추가
    if scenarios_in_dataset_data:
        cur.execute("USE avtail")
        cur.executemany(
            "INSERT IGNORE INTO scenarios_in_dataset (dataset_token, scenario_token) VALUES (%s, %s)",
            scenarios_in_dataset_data
        )

    conn.commit()
    cur.close()
    conn.close()

    print(f"[INFO] 새로 추가된 scenario ::::: {new_scenario_data}")
    print(f"[INFO] scenarios_in_dataset ::::: {scenarios_in_dataset_data}")


def visibility_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/visibility.yaml"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/preset.yaml"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                yaml_data = yaml.safe_load(f)
                data=[]
                preset_data=yaml_data['visibility']['contents']

                for visibility_data in preset_data:

                    if visibility_data['level']==1:

                        token="6"
                        level="v0-10"
                        description = visibility_data['description']

                    elif visibility_data['level']==2:

                        token="7"
                        level="v70-100"
                        description = visibility_data['description']
                
                    data.append((token, level, description))

                query="INSERT IGNORE INTO visibility (token, level, description) VALUES (%s,%s,%s)"

                cur.executemany(query, data)
                conn.commit()

    cur.close()
    conn.close()


# category 테이블 삽입용 데이터 생성 함수 (상하관계 경로 포함)
def generate_data(category_data, parent_path="", data=None):
    if data is None:
        data = []

    if isinstance(category_data, list):
        for item in category_data:
            generate_data(item, parent_path, data)

    elif isinstance(category_data, dict):
        name = category_data['value']
        description = category_data['description']
        default_scale = category_data.get("default_scale")

        # 상하관계를 나타내는 경로(path) 생성
        if parent_path:
            path = f"{parent_path}.{name}"
        else:
            path = name

        # default_scale이 없는 경우 NULL로 저장
        if default_scale is None:
            data.append((path, description, None))
        else:
            data.append((path, description, json.dumps(default_scale)))

        # 자식 노드 재귀 처리
        for child in category_data.get("children", []):
            generate_data(child, path, data)

    return data


def category_insert():
    # SFTP 접속
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    # MySQL 접속
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("USE adds")

    # category 테이블에서 이미 존재하는 name 목록 조회
    cur.execute("SELECT name FROM category")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB category name 목록 ::::: {existing_names}")

    # 전체 insert 대상 누적 리스트
    name_list = []

    with hook.get_conn() as sftp_client:
        for dir_name in kakao_directory_list:
            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/preset.yaml"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/preset.yaml"

            try:
                # preset.yaml 다운로드
                sftp_client.get(file_path, local_path)

                with open(local_path, 'r', encoding='utf-8') as f:
                    yaml_data = yaml.safe_load(f)
                    category_data = yaml_data.get('category', {}).get('contents', [])

                    # 상하관계 반영한 category 데이터 파싱
                    data = generate_data(category_data)

                    for path, description, default_scale in data:
                        if path not in existing_names:
                            token = secrets.token_hex(16)
                            name_list.append((token, path, description, default_scale))
                            existing_names.add(path)

            except Exception as e:
                print(f"[경고] preset.yaml 누락 또는 파싱 실패: {file_path} / {e}")
    
    print(f"기존 DB에 없는 새 category 데이터 ::::: {name_list}")

    # 한 번에 insert
    
    if name_list:
        print(f"기존 DB에 없는 새 category 데이터 ::::: {name_list}")
        query = "INSERT IGNORE INTO category (token, name, description, default_scale) VALUES (%s, %s, %s, %s)"
        cur.executemany(query, name_list)
        conn.commit()

    cur.close()
    conn.close()


def frame_annotation_insert():

    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # adds DB에서 attribute 조회
    cur.execute("use adds")
    cur.execute("SELECT name, token FROM attribute")
    attribute_data = cur.fetchall()
    attribute_dict = {name: token for name, token in attribute_data}

    # 이후 다시 작업 DB로 전환
    cur.execute("USE avtail")

    with hook.get_conn() as sftp_client:

        for dir_name in kakao_directory_list:

            local_dir = prepare_local_dir(dir_name)
            local_path = f"{local_dir}/frame_annotation.json"
            file_path = f"{kakao_remote_path}/{dir_name}/meta/frame_annotation.json"

            sftp_client.get(file_path, local_path)

            with open(local_path) as f:

                json_data = json.load(f)
                data = []
                print(len(json_data))

                # 현재 사용하는 DB 이름을 가져와 attribute 중복 제거 분기 나눠줌
                cur.execute("SELECT DATABASE()")
                current_db = cur.fetchone()[0]

                for i in range(len(json_data)):

                    frame_annotation_data = json_data[i]

                    uuid = frame_annotation_data['uuid']
                    token = uuid.replace("-","")

                    frame_data_uuid = frame_annotation_data['frame_data_uuid']
                    frame_data_token = frame_data_uuid.replace("-","")

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

                    frame_data_token = frame_data_uuid.replace("-","")

                    instance_uuid = frame_annotation_data['instance_uuid']
                    instance_token = instance_uuid.replace("-","")

                    """
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
                    """

                    annotation_type_name = frame_annotation_data['annotation_type_name']
                    is_lidar_synced = frame_annotation_data['is_lidar_synced']

                    # wlh, center, orientation, bbox_image3d 는 데이터에 따라서 geometry 데이터가 다르기때문에 값이 있을수도 없을 수도 있음
                    # 따라서 값이 없을때는 json 문자열로 바꿔주는 것이 아니라 None 값으로 놔둬야함
                    size_wlh = frame_annotation_data['geometry'].get('wlh')
                    if size_wlh:
                        size_wlh = json.dumps(size_wlh)

                    translation_center = frame_annotation_data['geometry'].get('center')
                    if translation_center:
                        translation_center = json.dumps(translation_center)

                    rotation_orientation = frame_annotation_data['geometry'].get('orientation')
                    if rotation_orientation:
                        rotation_orientation = json.dumps(rotation_orientation)

                    bbox_image2d = frame_annotation_data['geometry'].get('bbox_image2d')
                    if bbox_image2d:
                        bbox_image2d = frame_annotation_data['geometry']
                        bbox_image2d = json.dumps(bbox_image2d)

                    num_lidar_pts = frame_annotation_data['num_pts']


                    #attribute 의 데이터를 가공해서 name 을 만들어 준 후 name 에 해당하는 token 을 가져와서 배열에 넣어줌. attribute_tokens 이 attribute 에 해당하는 토큰 배열이므로 그 형태 만들어줌

                    if frame_annotation_data['attribute'] is not None:

                        # 데이터 한 행마다 attribute_tokens 배열은 초기화 되어야함
                        # 한 instance(= annotation 하나) 가 가지고있는 attribute 에 대한 데이터이기 때문
                        result_attribute_token = []

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
                                new_name = "pedestrian.moving"

                            elif new_name == "human.sitting" or new_name == "human.lying_down":
                                new_name = "pedestrian.sitting_lying_down"
                            
                            elif new_name == "rider.with_rider":
                                new_name = "cycle.with_rider"

                            elif new_name == "rider.without_rider":
                                new_name = "cycle.without_rider"

                            attribute_token = attribute_dict.get(new_name)
                            result_attribute_token.append(attribute_token)

                        result_attribute_token = json.dumps(result_attribute_token)

                    else:
                        result_attribute_token = None

                    
                    description = frame_annotation_data['description']

                    if frame_annotation_data['prev']:
                        prev = frame_annotation_data['prev'].replace("-","")
                    else:
                        prev = None

                    if frame_annotation_data['next']:
                        next = frame_annotation_data['next'].replace("-","")
                    else:
                        next = None


                    # 처음 데이터 넣어줄때는 frame_data_token 으로 frame_token select 하면 오래걸리기 때문에 frame_token 은 넣어주지않음
                    #모든 데이터 넣어준 후에 temp 테이블을 이용해서 한번에 insert 해줌
                    data.append((token, frame_data_token, instance_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image2d, num_lidar_pts, result_attribute_token, description, prev, next))
                    #print("data append")

                    #50000개 단위로 insert
                    if (i % 50000 == 0 and i > 0):

                        print("50000 data append end")

                        query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image2d, num_lidar_pts, attribute_tokens, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                        print("%d executemany start" %(i // 50000))
                        cur.executemany(query,data)
                        print("%d executemany end" %(i // 50000))

                        data = []

                # 나머지 데이터 삽입 ( 마지막 50000개까지 안 채워진 데이터 )
                query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image2d, num_lidar_pts, attribute_tokens, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                print("last executemany start")
                cur.executemany(query,data)
                print("last executemany end")

                #commit은 트랜잭션 고려해서 한 데이터셋에 대한 모든 데이터가 insert 됐을 때 마지막에 실행
                print("commit start")
                conn.commit()
                print("insert end")

            # **방금 쓴 파일·디렉터리 즉시 삭제**
            os.remove(local_path)
        

        #최종 frame_annotation 테이블와 동일한 temp 테이블을 만듦
        query = "CREATE TEMPORARY TABLE IF NOT EXISTS temp SELECT fa.token, fd.frame_token, fa.frame_data_token,fa.instance_token, fa.visibility_token,fa.annotation_type_name, fa.is_lidar_synced, fa.size_wlh, fa.translation_center, fa.rotation_orientation, fa.bbox_image2d, fa.num_lidar_pts, fa.attribute_tokens, fa.description, fa.prev, fa.`next` FROM frame_annotation fa JOIN frame_data fd ON fa.frame_data_token=fd.token"

        cur.execute(query)

        query = "DELETE FROM frame_annotation"
        cur.execute(query)
        conn.commit()

        #temp 테이블을 그대로 frame_annotation 에 넣어줌
        query = "INSERT INTO frame_annotation(token, frame_token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image2d, num_lidar_pts, attribute_tokens, description, prev, next) SELECT * FROM temp"

        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()


# get_chunk_parameters 와 attribute_frame_annotation_insert 태스크는 attribute 와 frame_annotation 의 JOIN 테이블에 insert 하는 태스크
# get_chunk_parameters 는 batch 수 구하기 위함.
@task
def get_chunk_parameters(**kwargs):
    
    def get_total_count():
        conn_info = MySqlHook.get_connection(conn_id = 'aws_mysql')
        connection = pymysql.connect(
            host = conn_info.host,
            user = conn_info.login,
            password = conn_info.password,
            db = 'avtail',
            port = conn_info.port,
            cursorclass = pymysql.cursors.DictCursor
        )
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM frame_annotation")
        result = cursor.fetchone()
        total_count = result['count']
        cursor.close()
        connection.close()

        return total_count
    
    total_count = get_total_count()
    

    ti = kwargs['ti']
    ti.xcom_push(key = 'total_count', value = total_count)

    num_tasks = (total_count + 199999) // 200000

    return [{"batch_number" : i} for i in range(num_tasks)]


def attribute_frame_annotation_insert(batch_number):

    conn_info = MySqlHook.get_connection(conn_id = 'aws_mysql')

    connection = pymysql.connect(
        host = conn_info.host,
        user = conn_info.login,
        password = conn_info.password,
        db = 'avtail',
        port = conn_info.port,
        cursorclass = pymysql.cursors.DictCursor,
        autocommit = False
    )
    
    page_size = 200000
    offset = batch_number * page_size

    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM frame_annotation LIMIT {offset}, {page_size}")
    results = cursor.fetchall()
    
    data = []

    for row in results:
        frame_annotation_token = row['token']
        if row['attribute_tokens']:
            attribute_token_list = json.loads(row['attribute_tokens'])
            for attribute_token in attribute_token_list:
                data.append((attribute_token, frame_annotation_token))

    query = "INSERT IGNORE INTO attribute_frame_annotation (attribute_token, frame_annotation_token) VALUES (%s, %s)"
    cursor.executemany(query, data)

    connection.commit()

    cursor.close()
    connection.close()


def copy_to_adds():
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    tables = [
        'annotation_type',
        'attribute',
        'attribute_frame_annotation',
        'calibrated_sensor',
        'ego_pose',
        'instance',
        'category',
        'lidarseg',
        'dataset',
        'log',
        'map',
        'log_map',
        'frame_annotation',
        'frame_data',
        'frame',
        'scenario',
        'scenarios_in_dataset',
        'sensor',
        'visibility'
    ]

    for table in tables:
        print(f"Copying table: {table}")
        query = f"""
            INSERT IGNORE INTO adds.{table}
            SELECT * FROM avtail.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()

with DAG('tail_light_detection_autonomous_vehicle_insert',
        default_args = default_args,
        schedule_interval = None,
        )as dag:
    
    annotation_type_insert_task = PythonOperator(task_id = 'annotation_type_insert', python_callable = annotation_type_insert)
    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    calibrated_sensor_insert_task = PythonOperator(task_id = 'calibrated_sensor_insert', python_callable = calibrated_sensor_insert)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    sensor_insert_task = PythonOperator(task_id = 'sensor_insert', python_callable = sensor_insert)
    dataset_insert_task = PythonOperator(task_id = 'dataset_insert', python_callable = dataset_insert)
    ego_pose_insert_task = PythonOperator(task_id = 'ego_pose_insert', python_callable = ego_pose_insert)
    frame_annotation_insert_task = PythonOperator(task_id = 'frame_annotation_insert', python_callable = frame_annotation_insert)
    frame_data_insert_task = PythonOperator(task_id = 'frame_data_insert', python_callable = frame_data_insert)
    frame_insert_task = PythonOperator(task_id = 'frame_insert', python_callable = frame_insert)
    instance_insert_task = PythonOperator(task_id = 'instance_insert', python_callable = instance_insert)
    log_insert_task = PythonOperator(task_id = 'log_insert', python_callable = log_insert)
    log_map_insert_task = PythonOperator(task_id = 'log_map_insert', python_callable = log_map_insert)
    scenario_insert_task = PythonOperator(task_id = 'scenario_insert', python_callable = scenario_insert)
    visibility_insert_task = PythonOperator(task_id = 'visibility_insert', python_callable = visibility_insert)

    chunk_params = get_chunk_parameters()

    attribute_frame_annotation_insert_task = PythonOperator.partial(task_id = 'process_batch', python_callable = attribute_frame_annotation_insert).expand(op_kwargs = chunk_params)
    cleanup_task = PythonOperator(task_id = "cleanup_local_dirs", python_callable = cleanup_dirs)
    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)


tasks = [annotation_type_insert_task, category_insert_task, dataset_insert_task, ego_pose_insert_task, frame_insert_task, log_insert_task, sensor_insert_task, visibility_insert_task]

# insert하려면 선행되어야하는 태스크들이 있어 올바른 순서가 필요함.
tasks >> attribute_insert_task >> log_map_insert_task >> scenario_insert_task >> calibrated_sensor_insert_task >> frame_data_insert_task >> instance_insert_task >> frame_annotation_insert_task >> chunk_params >> attribute_frame_annotation_insert_task >> cleanup_task >> copy_to_adds_task