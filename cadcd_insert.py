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
import numpy as np
from scipy.spatial.transform import Rotation as R
from datetime import datetime
from pyproj import Transformer
import math
from collections import defaultdict


default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes = 5),
}

cadcd_remote_path = '/data/cadcd'


# /labeled/3d_ann.json 은 미리 다운받아서 세팅

def prepare_local_dir(parent_dir, scene_dir):
    local_dir = f"cadcd/{parent_dir}/{scene_dir}"
    os.makedirs(local_dir, exist_ok=True)
    return local_dir


def cleanup_dirs():
    base_path = "cadcd"
    if os.path.exists(base_path):
        shutil.rmtree(base_path)


def download_all_ann_files():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    date_dirs = hook.list_directory(cadcd_remote_path)

    with hook.get_conn() as sftp_client:
        for date_dir in sorted(date_dirs):
            if date_dir in ('.', '..'):
                continue

            remote_date_path = f"{cadcd_remote_path}/{date_dir}"
            try:
                scene_dirs = hook.list_directory(remote_date_path)
            except Exception as e:
                print(f"[스킵] 디렉토리 접근 실패: {remote_date_path} / {e}")
                continue

            for scene_dir in sorted(scene_dirs):
                if scene_dir in ('.', '..'):
                    continue

                local_dir = prepare_local_dir(date_dir, scene_dir)
                remote_json_path = f"{remote_date_path}/{scene_dir}/3d_ann.json"
                local_json_path = f"{local_dir}/3d_ann.json"

                if not os.path.exists(local_json_path):  # 이미 있으면 스킵
                    try:
                        sftp_client.get(remote_json_path, local_json_path)
                        print(f"[다운로드 완료] {remote_json_path}")
                    except Exception as e:
                        print(f"[경고] 3d_ann.json 다운로드 실패: {remote_json_path} / {e}")


def download_all_novatel_files():
    """
    모든 씬에 대해 novatel data/*.txt, timestamps.txt 를 미리 다운로드
    """
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    date_dirs = hook.list_directory(cadcd_remote_path)

    with hook.get_conn() as sftp_client:
        for date_dir in sorted(date_dirs):
            if date_dir in ('.', '..'):
                continue
            remote_date_path = f"{cadcd_remote_path}/{date_dir}"
            try:
                scene_dirs = hook.list_directory(remote_date_path)
            except Exception as e:
                print(f"[스킵] 디렉토리 접근 실패: {remote_date_path} / {e}")
                continue

            for scene_dir in sorted(scene_dirs):
                if scene_dir in ('.', '..', 'calib'):
                    continue

                remote_pose_dir = f"{remote_date_path}/{scene_dir}/labeled/novatel"
                local_pose_dir = prepare_local_dir(date_dir, f"{scene_dir}/labeled/novatel")

                # timestamps.txt
                remote_ts_path = f"{remote_pose_dir}/timestamps.txt"
                local_ts_path = f"{local_pose_dir}/timestamps.txt"
                try:
                    sftp_client.get(remote_ts_path, local_ts_path)
                    print(f"[다운로드 완료] {remote_ts_path}")
                except Exception as e:
                    print(f"[경고] timestamps.txt 다운로드 실패: {remote_ts_path} / {e}")

                # data 디렉토리 내 모든 .txt 파일 다운로드
                try:
                    data_files = hook.list_directory(f"{remote_pose_dir}/data")
                    for file in sorted(data_files):
                        if not file.endswith(".txt"):
                            continue
                        remote_file = f"{remote_pose_dir}/data/{file}"
                        local_file = f"{local_pose_dir}/data/{file}"
                        os.makedirs(os.path.dirname(local_file), exist_ok=True)
                        sftp_client.get(remote_file, local_file)
                    print(f"[다운로드 완료] {remote_pose_dir}/data/*.txt")
                except Exception as e:
                    print(f"[경고] novatel/data/* 다운로드 실패: {remote_pose_dir} / {e}")


def convert_translation(lat, lon, altitude, undulation):
    # 좌표 변환기 (WGS84 → UTM Zone 17N)
    transformer = Transformer.from_crs("epsg:4326", "epsg:32617", always_xy=True)

    x, y = transformer.transform(lon, lat)
    z = altitude + undulation
    return [x, y, z]

def convert_rotation(roll, pitch, yaw):
    r = R.from_euler('xyz', [roll, pitch, yaw], degrees=True)
    return r.as_quat().tolist()  # [qx, qy, qz, qw]


def yaw_to_quaternion(yaw):
    half_yaw = yaw / 2.0
    return [0.0, 0.0, math.sin(half_yaw), math.cos(half_yaw)]


def category_insert():
    # SFTP 연결
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    date_dirs = hook.list_directory(cadcd_remote_path)

    # MySQL 연결
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    # 기존 category.name 모두 조회
    cur.execute("SELECT name FROM category")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB의 category ::::: {existing_names}")

    name_list = []
    existing_names = set()  # 중복 label 방지용

    with hook.get_conn() as sftp_client:
        for date_dir in sorted(date_dirs):  # 날짜 디렉토리 정렬하여 순회 (예: 2018_03_06, 2018_03_07, ...)
            if date_dir in ('calib'):
                continue

            remote_date_path = f"{cadcd_remote_path}/{date_dir}"
            try:
                scene_dirs = hook.list_directory(remote_date_path)
            except Exception as e:
                print(f"[스킵] 디렉토리 접근 실패: {remote_date_path} / {e}")
                continue

            for scene_dir in sorted(scene_dirs):  # 씬 디렉토리 정렬하여 순회 (예: 0001, 0002, ...)
                if scene_dir in ('.', '..'):
                    continue

                remote_json_path = f"{remote_date_path}/{scene_dir}/3d_ann.json"
                local_dir = prepare_local_dir(date_dir, scene_dir)
                local_json_path = f"{local_dir}/3d_ann.json"

                try:
                    if not os.path.exists(local_json_path):
                        print(f"[스킵] 로컬에 3d_ann.json 없음: {local_json_path}")
                        continue

                    with open(local_json_path, 'r') as f:
                        data = json.load(f)

                    for frame in data:
                        cuboids = frame.get("cuboids", [])
                        for cuboid in cuboids:
                            label = cuboid.get("label")
                            if label:
                                name = label  # 명시적으로 name으로 사용
                                if name not in existing_names:
                                    token = secrets.token_hex(16)
                                    name_list.append((token, name, None, None))
                                    existing_names.add(name)

                except Exception as e:
                    print(f"[경고] 3d_ann.json 파싱 실패: {remote_json_path} / {e}")

    # 최종 insert
    if name_list:
        insert_query = """
            INSERT INTO category (token, name, description, default_scale)
            VALUES (%s, %s, %s, %s)
        """
        cur.executemany(insert_query, name_list)
        conn.commit()
        print(f"[완료] category 테이블에 {len(name_list)}건 삽입 완료")

    # ===== [추가] label 정리 작업 =====
    delete_labels = {
        "Bus", "Pedestrian_With_Object", "Truck", "Pedestrian",
        "Traffic_Guidance_Objects", "Bicycle", "Car", "Animals"
    }

    rename_labels = {
        "Horse_and_Buggy": "vehicle.horse_and_buggy",
        "Garbage_Containers_on_Wheels": "movable_object.garbage_containers_on_wheels"
    }

    label_map = {
        "Bicycle": "vehicle.bicycle",
        "Horse_and_Buggy": "vehicle.horse_and_buggy",
        "Car": "vehicle.car",
        "Animals": "animal",
        "Garbage_Containers_on_Wheels": "movable_object.garbage_containers_on_wheels"
    }

    # 이름 변경
    for original_name, new_name in rename_labels.items():
        cur.execute("""
            UPDATE category
            SET name = %s
            WHERE name = %s
        """, (new_name, original_name))
        print(f"[수정] category 이름 변경: {original_name} → {new_name}")

    # 삭제
    delete_query = """
        DELETE FROM category
        WHERE name IN (%s)
    """ % ",".join(["%s"] * len(delete_labels))

    cur.execute(delete_query, tuple(delete_labels))
    conn.commit()
    print(f"[삭제] 삭제된 category: {delete_labels}")


    cur.close()
    conn.close()


# NOTE: existing_names adds DB에서 조회? OR cadcd에서 조회?
def attribute_insert():
    # SFTP 연결
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    date_dirs = hook.list_directory(cadcd_remote_path)

    # MySQL 연결
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    # 기존 attribute.name 모두 조회
    cur.execute("SELECT name FROM attribute")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB의 attribute ::::: {existing_names}")

    name_list = set()  # 최종 insert 대상

    with hook.get_conn() as sftp_client:
        for date_dir in sorted(date_dirs):  # 날짜 디렉토리 정렬하여 순회
            if date_dir in ('calib'):
                continue

            remote_date_path = f"{cadcd_remote_path}/{date_dir}"
            try:
                scene_dirs = hook.list_directory(remote_date_path)
            except Exception as e:
                print(f"[스킵] 디렉토리 접근 실패: {remote_date_path} / {e}")
                continue

            for scene_dir in sorted(scene_dirs):  # 씬 디렉토리 정렬하여 순회
                if scene_dir in ('.', '..'):
                    continue

                remote_json_path = f"{remote_date_path}/{scene_dir}/3d_ann.json"
                local_dir = prepare_local_dir(date_dir, scene_dir)
                local_json_path = f"{local_dir}/3d_ann.json"

                try:
                    if not os.path.exists(local_json_path):
                        print(f"[스킵] 로컬에 3d_ann.json 없음: {local_json_path}")
                        continue

                    with open(local_json_path, 'r') as f:
                        data = json.load(f)

                    for frame in data:
                        cuboids = frame.get("cuboids", [])
                        for cuboid in cuboids:
                            label = cuboid.get("label")
                            attr_dict = cuboid.get("attributes", {})

                            for key, val in attr_dict.items():
                                # 기본 형태: key.value
                                attr_name = f"{key}.{val}"

                                # "state" 항목이면 label도 앞에 붙임: label.state.value
                                if key == "state" and label:
                                    attr_name = f"{label}.state.{val}"
                                
                                if key == "rider_state" and label:
                                    attr_name = f"{label}.rider_state.{val}"

                                if attr_name not in existing_names:
                                    name_list.add(attr_name)
                                    existing_names.add(attr_name)

                except Exception as e:
                    print(f"[경고] 3d_ann.json 파싱 실패: {remote_json_path} / {e}")

    print(f"기존 attribute 테이블에 없는 데이터 ::::: {name_list}")

    # insert 수행
    if name_list:
        data = [(secrets.token_hex(16), name) for name in name_list]
        cur.executemany("INSERT INTO attribute (token, name) VALUES (%s, %s)", data)
        conn.commit()
        print(f"[완료] attribute 테이블에 {len(name_list)}건 삽입 완료")


    # === [1] 이름 변경 (유지할 것만) ===
    label_rename_map = {
        "Bus.state.Moving": "vehicle.bus.moving",
        "Bus.state.Stopped": "vehicle.bus.stopped",
        "Bicycle.state.Moving": "cycle.moving",
        "Bus.state.Parked": "vehicle.bus.parked",
        "Truck.state.Moving": "vehicle.truck.moving",
        "Car.state.Moving": "vehicle.car.moving",
        "Car.state.Parked": "vehicle.car.parked",
        "Truck.state.Stopped": "vehicle.truck.stopped",
        "Truck.state.Parked": "vehicle.truck.parked",
        "Bicycle.state.Parked": "cycle.parked",
        "Car.state.Stopped": "vehicle.car.stopped",
        "Horse_and_Buggy.state.Stopped": "vehicle.Horse_and_Buggy.stopped"
    }

    
    update_query = """
        UPDATE attribute
        SET name = %s
        WHERE name = %s
    """

    update_data = [(new_name, old_name) for old_name, new_name in label_rename_map.items()]

    cur.executemany(update_query, update_data)
    conn.commit()

    # === [2] 삭제할 attribute 항목 ===
    names_to_delete = [
        "Bicycle.rider_state.Without_Rider",
        "Bicycle.rider_state.With_Rider"
    ]
    cur.execute(
        "DELETE FROM attribute WHERE name IN (%s)" % ",".join(["%s"] * len(names_to_delete)),
        tuple(names_to_delete)
    )
    conn.commit()

    # === [3] 삭제할 category 관련 항목 ===
    cur.execute("""
        DELETE FROM attribute WHERE name IN ('age.Child', 'age.Adult')
    """)
    conn.commit()

    # === [4] category 테이블로 옮길 항목들 ===
    label_map_attr_to_category = {
        "truck_type.Construction_Truck": "vehicle.truck.construction_truck",
        "traffic_guidance_type.Moveable": "traffic_guidance_type.moveable",
        "bus_type.Standard_School_Bus": "vehicle.bus.standard_school_bus",
        "truck_type.Garbage_Truck": "vehicle.truck.garbage_truck",
        "truck_type.Semi_Truck": "vehicle.truck.semi_truck",
        "bus_type.Van_School_Bus": "vehicle.bus.van_school_bus",
        "bus_type.Transit_Bus": "vehicle.bus.transit_bus",
        "traffic_guidance_type.Permanent": "traffic_guidance_type.permanent",
        "truck_type.Emergency_Truck": "vehicle.truck.emergency_truck",
        "truck_type.Pickup_Truck": "vehicle.truck.pickup_truck",
        "bus_type.Coach_Bus": "vehicle.bus.coach_bus",
        "truck_type.Snowplow_Truck": "vehicle.truck.snowplow_truck"
    }

    category_values = list(label_map_attr_to_category.values())

    # category 테이블에서 기존 데이터 조회
    cur.execute("SELECT name FROM category")
    existing_category_names = {row[0] for row in cur.fetchall()}

    # insert 대상만 추림
    insert_category_data = []
    for original, converted in label_map_attr_to_category.items():
        if converted not in existing_category_names:
            token = secrets.token_hex(16)
            insert_category_data.append((token, converted, None, None))

    if insert_category_data:
        cur.executemany("""
            INSERT INTO category (token, name, description, default_scale)
            VALUES (%s, %s, %s, %s)
        """, insert_category_data)
        conn.commit()
        print(f"[완료] category 테이블에 {len(insert_category_data)}건 삽입 완료")

    # category로 이동한 항목은 attribute에서 삭제
    cur.execute(
        "DELETE FROM attribute WHERE name IN (%s)" % ",".join(["%s"] * len(label_map_attr_to_category)),
        tuple(label_map_attr_to_category.keys())
    )
    conn.commit()

    # === cadcd에서 추출된 attribute 테이블을 copy_to_adds 함수가 아닌 여기서 adds DB로 옮김
    # frame_annotation 테이블의 attribute_tokens 값이 adds 기준으로 조회되어야하기 때문.
    query = f"""
            INSERT IGNORE INTO adds.attribute
            SELECT * FROM cadcd.attribute
        """
    cur.execute(query)
    conn.commit()

    # === cadcd에서 추출된 category copy_to_adds 함수가 아닌 여기서 adds DB로 옮김
    # instance 테이블의 category_token 값이 adds 기준으로 조회되어야하기 때문.
    # attribute_insert에서도 category가 채워지므로 category_insert 함수에서 adds DB로 옮기지 않음.
    # 최종본 category 테이블을 adds로 옮겨야하므로 attribute_insert 함수에서 옮김
    query = f"""
            INSERT IGNORE INTO adds.category
            SELECT * FROM cadcd.category
        """
    cur.execute(query)
    conn.commit()

    cur.close()
    conn.close()


def sensor_insert(ti=None):
    """
    sensor 테이블에 cam00 ~ cam07, lidar 정보 insert
    생성된 token은 XCom으로 전달 (key='sensor_token_map')
    XCom key는 "00" ~ "07", "lidar" 형식
    """

    sensor_list = []
    sensor_token_map = {}

    # CAM00 ~ CAM07 센서 등록
    for i in range(8):
        channel = f"CAM0{i}"
        token = secrets.token_hex(16)
        modality = "camera"

        sensor_list.append((token, channel, modality))
        sensor_token_map[f"{i:02d}"] = token  # "00", "01", ..., "07"

    # LiDAR 센서 등록
    lidar_token = secrets.token_hex(16)
    sensor_list.append((lidar_token, "LIDAR", "lidar"))
    sensor_token_map["lidar"] = lidar_token

    # DB 연결 및 insert
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    query = """
        INSERT IGNORE INTO sensor (
            token, channel, modality
        ) VALUES (%s, %s, %s)
    """
    cur.executemany(query, sensor_list)
    conn.commit()
    cur.close()
    conn.close()

    # XCom push
    if ti:
        ti.xcom_push(key = 'sensor_token_map', value = sensor_token_map)
        print(f"[XCom] sensor_token_map push 완료: {sensor_token_map}")

    print(f"[완료] sensor 테이블에 {len(sensor_list)}건 insert 완료")
    

def scenario_insert():
    """
    scenario 테이블에 단일 레코드 insert
    token은 랜덤 생성, description은 고정 값
    """
    # MySQL 연결
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    # 데이터 정의
    token = secrets.token_hex(16)
    description = "Snow, various levels of traffic, variety of vehicles"

    # insert 실행
    try:
        cur.execute(
            """
            INSERT INTO scenario (token, description)
            VALUES (%s, %s)
            """,
            (token, description)
        )
        conn.commit()
        print(f"[완료] scenario 테이블에 레코드 삽입 완료: {token}")
    except Exception as e:
        print(f"[에러] scenario insert 실패: {e}")

    cur.close()
    conn.close()


def integral_insert(ti = None):
    """
    calibrated_sensor, scene (구 dataset), dataset (어떤 데이터셋인지 정보), log, frame
    calibrated_sensor 테이블에 카메라 및 LiDAR 캘리브레이션 정보 insert
    sensor_token은 XCom으로 전달된 sensor_insert 결과 사용
    """

    # sensor_insert에서 전달한 sensor_token_map 가져오기
    sensor_token_map = ti.xcom_pull(task_ids='sensor_insert', key='sensor_token_map')
    if not sensor_token_map:
        print("[에러] sensor_token_map을 XCom에서 불러오지 못함")
        return

    # SFTP 및 MySQL 연결
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    date_dirs = hook.list_directory(cadcd_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    # === dataset 테이블 insert
    dataset_token = secrets.token_hex(16)
    name = 'CADC'

    query = "INSERT INTO dataset (token, name) VALUES (%s, %s)"
    cur.execute(query, (dataset_token, name))

    # === attribute cadcd 표현 방식 rename map
    rename_map_cadcd = {
        "Bus.state.Moving": "vehicle.bus.moving",
        "Bus.state.Stopped": "vehicle.bus.stopped",
        "Bicycle.state.Moving": "cycle.moving",
        "Bus.state.Parked": "vehicle.bus.parked",
        "Truck.state.Moving": "vehicle.truck.moving",
        "Car.state.Moving": "vehicle.car.moving",
        "Car.state.Parked": "vehicle.car.parked",
        "Truck.state.Stopped": "vehicle.truck.stopped",
        "Truck.state.Parked": "vehicle.truck.parked",
        "Bicycle.state.Parked": "cycle.parked",
        "Car.state.Stopped": "vehicle.car.stopped",
        "Horse_and_Buggy.state.Stopped": "vehicle.Horse_and_Buggy.stopped"
    }

    # === attribute adds 표현 방식 rename map
    rename_map_adds = {
        "Bus.state.Moving": "vehicle.moving",
        "Bus.state.Stopped": "vehicle.stopped",
        "Bicycle.state.Moving": "vehicle.moving",
        "Bus.state.Parked": "vehicle.parked",
        "Truck.state.Moving": "vehicle.moving",
        "Car.state.Moving": "vehicle.moving",
        "Car.state.Parked": "vehicle.parked",
        "Truck.state.Stopped": "vehicle.stopped",
        "Truck.state.Parked": "vehicle.parked",
        "Bicycle.rider_state.Without_Rider": "cycle.without_rider",
        "Bicycle.state.Parked": "vehicle.parked",
        "Car.state.Stopped": "vehicle.stopped",
        "Bicycle.rider_state.With_Rider": "cycle.with_rider"
    }

    # === label에서 category 추출
    label_map_label = {
        "Bicycle": "vehicle.bicycle",
        "Horse_and_Buggy": "vehicle.horse_and_buggy",
        "Car": "vehicle.car",
        "Animals": "animal",
        "Garbage_Containers_on_Wheels": "movable_object.garbage_containers_on_wheels"
    }

    # === attributes에서 category 추출
    label_map_attr = {
        "truck_type.Construction_Truck": "vehicle.truck.construction_truck",
        "traffic_guidance_type.Moveable": "traffic_guidance_type.moveable",
        "bus_type.Standard_School_Bus": "vehicle.bus.standard_school_bus",
        "age.Child": "human.pedestrian.child",
        "truck_type.Garbage_Truck": "vehicle.truck.garbage_truck",
        "truck_type.Semi_Truck": "vehicle.truck.semi_truck",
        "bus_type.Van_School_Bus": "vehicle.bus.van_school_bus",
        "bus_type.Transit_Bus": "vehicle.bus.transit_bus",
        "traffic_guidance_type.Permanent": "traffic_guidance_type.permanent",
        "truck_type.Emergency_Truck": "vehicle.truck.emergency_truck",
        "age.Adult": "human.pedestrian.adult",
        "truck_type.Pickup_Truck": "vehicle.truck.pickup_truck",
        "bus_type.Coach_Bus": "vehicle.bus.coach_bus",
        "truck_type.Snowplow_Truck": "vehicle.truck.snowplow_truck"
    }
    # 통합 DB에 있는 attribute랑 category 와 겹치면 삭제. 그리고 integral_insert 함수 즉 category_token, attribute_tokens 조회해야할때는 cadcd 데이터셋에서의 이름을 통합 DB에서 사용하는 이름으로 맞춰주어야함. 통합 DB에서 조회했을 때 조회되도록. 해주어야함. 이것만 신경쓰면 오류 안남.
    # Pedestrian 인 경우는 attribute로 category 조회함. age.Adult 인데, 만약 attribute name이 이런 경우라면 통합 DB에서 검색되도록 pedestrian.age.adult 로 바꿔서 조회함.

    # 실제 최종본에서는 이 코드로 attribute_token_dict, category_token_dict 조회함.
    
    # adds DB에서 attribute 조회 (조회 전 DB 바꾸기)
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM attribute")
    attribute_token_dict = dict(cur.fetchall())

    print(f"[attribute_token_dict 확인] ======= {attribute_token_dict}")

    cur.execute("SELECT name, token FROM category")
    category_token_dict = dict(cur.fetchall())

    print(f"[category_token_dict 확인] ======= {category_token_dict}")
    
    """
    # === ADDs DB에서 attribute, category 가져오기
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM attribute")
    adds_attributes = dict(cur.fetchall())

    cur.execute("SELECT name, token FROM category")
    adds_category = dict(cur.fetchall())

    # === CADCD DB에서 attribute 가져오기
    cur.execute("USE cadcd")
    cur.execute("SELECT name, token FROM attribute")
    cadcd_attributes = dict(cur.fetchall())

    attribute_token_dict = adds_attributes.copy()

    # CADCD DB 값 추가 (ADDs에 없는 경우만)
    attribute_token_dict.update({
        k: v for k, v in cadcd_attributes.items() if k not in attribute_token_dict
    })

    print(f"[attribute_token_dict 확인] ======= {attribute_token_dict}")

    cur.execute("SELECT name, token FROM category")
    cadcd_category = dict(cur.fetchall())

    category_token_dict = adds_category.copy()

    # CADCD DB 값 추가 (ADDs에 없는 경우만)
    category_token_dict.update({
        k: v for k, v in cadcd_category.items() if k not in category_token_dict
    })

    print(f"[category_token_dict 확인] ======= {category_token_dict}")
    """

    # 다시 cadcd DB로 전환해서 insert 진행
    cur.execute("USE cadcd")

    with hook.get_conn() as sftp_client:
        for date_dir in sorted(date_dirs):      # 2018_03_06, 2018_03_07 .. 날짜 순회
            if date_dir in ('.', '..'):
                continue

            remote_date_path = f"{cadcd_remote_path}/{date_dir}"    # /data/cadcd/2018_03_06
            remote_calib_path = f"{remote_date_path}/calib"
            local_dir = prepare_local_dir(date_dir, "calib")


            try:
                calib_files = hook.list_directory(remote_calib_path)
            except Exception as e:
                print(f"[스킵] calib 디렉토리 없음: {remote_calib_path} / {e}")
                continue

            # === extrinsics.yaml 처리 ===
            extr_path_remote = f"{remote_calib_path}/extrinsics.yaml"
            extr_path_local = f"{local_dir}/extrinsics.yaml"
            try:
                sftp_client.get(extr_path_remote, extr_path_local)
                with open(extr_path_local, 'r') as f:
                    extr = yaml.safe_load(f)
            except Exception as e:
                print(f"[스킵] extrinsics.yaml 누락: {extr_path_remote} / {e}")
                continue

            T_bl_lidar = np.array(extr['T_BASELINK_LIDAR'])
            data = []

            calibrated_sensor_map = {}      # 씬 단위 calibrated_sensor 토큰 맵핑 저장용
            # === CAM00 ~ CAM07 처리 ===
            for i in range(8):
                cam_key = f"T_LIDAR_CAM0{i}"
                yaml_file = f"0{i}.yaml"
                remote_yaml_path = f"{remote_calib_path}/{yaml_file}"
                local_yaml_path = f"{local_dir}/{yaml_file}"

                if cam_key not in extr:
                    continue

                # 내부 파라미터 (intrinsic)
                try:
                    sftp_client.get(remote_yaml_path, local_yaml_path)
                    with open(local_yaml_path, 'r') as f:
                        intr = yaml.safe_load(f)
                    intrinsic = intr.get("camera_matrix", {}).get("data", None)
                except Exception as e:
                    print(f"[스킵] 내부 파라미터 누락: {remote_yaml_path} / {e}")
                    intrinsic = None

                # extrinsics 변환
                T_lidar_cam = np.array(extr[cam_key])
                T_bl_cam = T_bl_lidar @ T_lidar_cam
                translation = T_bl_cam[:3, 3].tolist()
                q_xyzw = R.from_matrix(T_bl_cam[:3, :3]).as_quat()
                rotation = [q_xyzw[3], *q_xyzw[:3]]
                height_from_ground = None

                # sensor_token = "00" ~ "07" 으로부터 lookup
                sensor_token_key = f"{i:02d}"
                sensor_token = sensor_token_map.get(sensor_token_key)
                if not sensor_token:
                    print(f"[경고] sensor_token_map에 {sensor_token_key} 키가 없습니다. 건너뜀")
                    continue

                token = secrets.token_hex(16)
                data.append((
                    token,
                    sensor_token,
                    json.dumps(translation),
                    json.dumps(rotation),
                    json.dumps(intrinsic) if intrinsic else None,
                    height_from_ground
                ))

                # 센서명으로 calibrated_sensor_token 매핑 저장
                calibrated_sensor_map[f"image_0{i}"] = token

            # === LiDAR 처리 ===
            lidar_key = "T_LIDAR_LIDAR"
            try:
                T_lidar_lidar = np.array(extr[lidar_key]) if lidar_key in extr else np.eye(4)
                T_bl_lidar_final = T_bl_lidar @ T_lidar_lidar
                translation = T_bl_lidar_final[:3, 3].tolist()
                q_xyzw = R.from_matrix(T_bl_lidar_final[:3, :3]).as_quat()
                rotation = [q_xyzw[3], *q_xyzw[:3]]
                height_from_ground = None

                sensor_token = sensor_token_map.get("lidar")
                if not sensor_token:
                    print(f"[경고] sensor_token_map에 'lidar' 키가 없습니다. 건너뜀")
                else:
                    token = secrets.token_hex(16)
                    data.append((
                        token,
                        sensor_token,
                        json.dumps(translation),
                        json.dumps(rotation),
                        None,  # intrinsic 없음
                        height_from_ground
                    ))

                    # LiDAR용 매핑 저장
                    calibrated_sensor_map["lidar_points"] = token

            except Exception as e:
                print(f"[스킵] LiDAR 캘리브레이션 실패: {date_dir} / {e}")

            # === DB insert ===
            if data:
                query = """
                    INSERT IGNORE INTO calibrated_sensor (
                        token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                try:
                    cur.executemany(query, data)
                    print(f"[완료] calibrated_sensor 테이블에 {len(data)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] calibrated_sensor insert 실패: {date_dir} / {e}")
            
            # === 각 date_dir 내 씬 디렉토리별 frame + log + scene 처리 ===
            frame_rows = []
            scene_rows = []
            log_rows = []
            ego_pose_rows = []

            # frame_annotation, instance 테이블 용
            all_instance_rows = []
            all_annotation_rows = []
            seen_instance_uuids = set()
            uuid_to_annotations = defaultdict(list)
            instance_token_to_info = {}

            for scene_dir in hook.list_directory(remote_date_path):     # 현재 date_dir 아래에 있는 씬 디렉토리들 순회 (0001, 0002, ...)
                if scene_dir in ('calib'):
                    continue

                frame_token_map = {}  
                ego_pose_token_map = {}         # frame_index:ego_pose_token 매핑

                remote_scene_path = f"{remote_date_path}/{scene_dir}"   # /data/cadcd/2018_03_06/0001
                local_scene_dir = prepare_local_dir(date_dir, scene_dir)

                # 3d_ann.json 가져오기 
                ann_local = f"{local_scene_dir}/3d_ann.json"
                if not os.path.exists(ann_local):
                    print(f"[스킵] 로컬에 3d_ann.json 없음: {ann_local}")
                    continue

                try:
                    with open(ann_local, 'r') as f:
                        ann = json.load(f)      # 씬 단위(0001, ...) 3d_ann.json
                except Exception as e:
                    print(f"[스킵] 3d_ann.json 파싱 실패: {scene_dir} / {e}")
                    continue

                frame_count = len(ann)
                if frame_count == 0:
                    print(f"[스킵] cuboids 비어 있음: {scene_dir}")
                    continue

                # timestamps.txt 가져오기
                ts_remote = f"{remote_scene_path}/labeled/image_00/timestamps.txt"
                ts_local = f"{local_scene_dir}/timestamps.txt"
                try:
                    sftp_client.get(ts_remote, ts_local)
                    with open(ts_local, 'r') as f:
                        timestamps = [line.strip() for line in f if line.strip()]
                except Exception as e:
                    print(f"[스킵] timestamps.txt 누락: {scene_dir} / {e}")
                    continue

                if len(timestamps) < frame_count:
                    print(f"[경고] timestamps 수 ({len(timestamps)}) < cuboids 수 ({frame_count}) in {scene_dir}, 프레임 일부 누락될 수 있음")

                # === log_token, scene_token, frame_token 생성 ===
                log_token = secrets.token_hex(16)
                scene_token = secrets.token_hex(16)
                tokens = [secrets.token_hex(16) for _ in range(frame_count)]

                # === log 테이블용 데이터 추가 ===
                try:
                    first_ts_line = timestamps[0]
                    date_captured = first_ts_line[:26]  # DATETIME(6) 대응
                except Exception as e:
                    print(f"[경고] log date_captured 추출 실패: {scene_dir} / {e}")
                    continue

                default_device = "Autonomoose"
                default_location = "canada-waterloo"
                log_rows.append((log_token, dataset_token, default_device, date_captured, default_location))

                # === frame 데이터 ===
                for i in range(frame_count):
                    prev_token = tokens[i - 1] if i > 0 else None
                    cur_token = tokens[i]
                    next_token = tokens[i + 1] if i < frame_count - 1 else None

                    # timestamp 변환
                    try:
                        ts_str = timestamps[i]
                        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f000")
                        unix_ts = dt.timestamp()
                    except Exception as e:
                        print(f"[경고] timestamp 변환 실패 ({ts_str}) in {scene_dir} / {e}")
                        unix_ts = None

                    # 인덱스 저장: "0000000000" 형태
                    frame_index = f"{i:010d}"
                    frame_token_map[frame_index] = cur_token        # 씬 단위 (0001, ...) frame token map

                    frame_rows.append((
                        cur_token,
                        unix_ts,
                        scene_token,  # 연결된 scene_token 사용
                        prev_token,
                        next_token
                    ))

                # === scene 데이터 ===
                first_frame_token = tokens[0]
                last_frame_token = tokens[-1]
                base_path = f"cadcd/{date_dir}/{scene_dir}"
                num_frames = frame_count

                scene_rows.append((
                    scene_token,
                    log_token,
                    base_path,
                    num_frames,
                    first_frame_token,
                    last_frame_token
                ))

                # === ego_pose 처리 ===

                # novatel 폴더의 로컬 경로
                pose_local_dir = f"cadcd/{date_dir}/{scene_dir}/labeled/novatel"
                pose_ts_local = f"{pose_local_dir}/timestamps.txt"

                # 1. novatel/timestamps.txt 읽기
                try:
                    with open(pose_ts_local, "r") as f:
                        pose_timestamps = [line.strip() for line in f if line.strip()]
                except Exception as e:
                    print(f"[스킵] novatel timestamps.txt 누락: {scene_dir} / {e}")
                    pose_timestamps = []

                # 2. 각 프레임에 대응하는 novatel/data/{index}.txt 파일 순회
                for i in range(frame_count):
                    pose_file_local = f"{pose_local_dir}/data/{i:010d}.txt"

                    try:
                        # 3. 파일 열고 데이터 파싱 (총 16개 값 중 필요한 7개만 사용)
                        with open(pose_file_local, "r") as f:
                            vals = f.read().strip().split()

                        # 필수 항목 추출 및 float 변환
                        lat, lon, alt, undu = map(float, vals[:4])          # 위치 + 지오이드 보정값
                        roll, pitch, yaw = map(float, vals[7:10])           # 자세 (deg)

                        # 4. 위도/경도 → UTM 기반 [x, y, z] 변환
                        translation = convert_translation(lat, lon, alt, undu)

                        # 5. 오일러 각 → 쿼터니언 [qx, qy, qz, qw]
                        rotation = convert_rotation(roll, pitch, yaw)

                        # 6. 타임스탬프 문자열 → 유닉스 시간 (초 단위 float)
                        ts_trimmed = pose_timestamps[i][:26]
                        timestamp = datetime.strptime(ts_trimmed, "%Y-%m-%d %H:%M:%S.%f").timestamp()

                        ego_pose_token = secrets.token_hex(16)
                        frame_index = f"{i:010d}"           # frame 인덱스 문자열화

                        # 7. ego_pose 레코드 구성
                        ego_pose_rows.append((
                            ego_pose_token,         # ego_pose.token
                            timestamp,                     # 유닉스 시간
                            json.dumps(translation),       # UTM 좌표
                            json.dumps(rotation),           # 쿼터니언 회전
                            "vehicle"
                        ))
                        ego_pose_token_map[frame_index] = ego_pose_token

                    except Exception as e:
                        print(f"[경고] ego_pose 파일 처리 실패: {scene_dir} / {i:010d} / {e}")


                # === frame_data 사전 준비 === (frame_data token들, timestamp들)
                sensor_list = [f"image_0{i}" for i in range(8)] + ["lidar_points"]

                timestamps_map = {}
                sensor_tokens_map = {}

                for sensor_name in sensor_list:
                    if sensor_name not in calibrated_sensor_map:
                        print(f"[경고] calibrated_sensor_token 없음: {sensor_name}")
                        continue

                    # timestamps.txt 경로 지정 및 유닉스 타임스탬프 변환
                    ts_remote = f"{remote_scene_path}/labeled/{sensor_name}/timestamps.txt"
                    ts_local = f"{local_scene_dir}/{sensor_name}_timestamps.txt"

                    try:
                        sftp_client.get(ts_remote, ts_local)
                        with open(ts_local, "r") as f:
                            lines = [line.strip() for line in f if line.strip()]

                        unix_timestamps = []
                        for line in lines:
                            try:
                                dt = datetime.strptime(line[:26], "%Y-%m-%d %H:%M:%S.%f")
                                unix_ts = dt.timestamp()
                                unix_timestamps.append(unix_ts)
                            except Exception as e:
                                print(f"[경고] {sensor_name} timestamp 변환 실패: {line} / {e}")

                        timestamps_map[sensor_name] = unix_timestamps
                        if len(unix_timestamps) != frame_count:
                            print(f"[경고] {sensor_name} timestamp 수 ({len(unix_timestamps)}) != frame_count({frame_count})")

                    except Exception as e:
                        print(f"[스킵] {sensor_name} timestamps.txt 누락: {scene_dir} / {e}")
                        timestamps_map[sensor_name] = []

                    # 센서별 frame 개수만큼 token 미리 생성
                    sensor_tokens_map[sensor_name] = [secrets.token_hex(16) for _ in range(frame_count)]

                # === 실제 처리 ===
                frame_data_rows = []

                for i in range(frame_count):
                    frame_index = f"{i:010d}"
                    frame_token = frame_token_map.get(frame_index)
                    ego_pose_token = ego_pose_token_map.get(frame_index)

                    if not frame_token or not ego_pose_token:
                        print(f"[경고] frame_token 또는 ego_pose_token 누락: {frame_index}")
                        continue

                    for sensor_name in sensor_list:
                        tokens = sensor_tokens_map.get(sensor_name)
                        timestamps = timestamps_map.get(sensor_name)
                        calibrated_token = calibrated_sensor_map.get(sensor_name)  # ✅ 올바른 수정

                        if not tokens or not timestamps or not calibrated_token:
                            print(f"[경고] {sensor_name} 사전 맵 누락: {scene_dir}")
                            continue

                        try:
                            cur_token = tokens[i]
                            prev_token = tokens[i - 1] if i > 0 else None
                            next_token = tokens[i + 1] if i < frame_count - 1 else None

                            timestamp = timestamps[i]

                            is_key_frame = 1
                            height = 1024
                            width = 1280

                            if "image" in sensor_name:
                                file_name = f"{sensor_name}/data/{frame_index}.png"
                                file_format = "png"
                            else:
                                file_name = f"{sensor_name}/data/{frame_index}.bin"
                                file_format = "bin"

                            frame_data_rows.append((
                                cur_token,
                                frame_token,
                                ego_pose_token,
                                calibrated_token,
                                timestamp,
                                is_key_frame,
                                height,
                                width,
                                file_name,
                                file_format,
                                prev_token,
                                next_token
                            ))

                        except Exception as e:
                            print(f"[에러] {sensor_name} 처리 중 오류 (index={i}) / {e}")
                
                # frame_data 테이블 insert
                if frame_data_rows:
                    query = """
                        INSERT IGNORE INTO frame_data (
                            token,
                            frame_token,
                            ego_pose_token,
                            calibrated_sensor_token,
                            timestamp,
                            is_key_frame,
                            height,
                            width,
                            file_name,
                            file_format,
                            prev,
                            next
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    try:
                        cur.executemany(query, frame_data_rows)
                        print(f"[완료] frame_data 테이블에 {len(frame_data_rows)}건 insert 완료")
                    except Exception as e:
                        print(f"[에러] frame_data insert 실패: {e}")

                # === frame_annotation 처리 ===
                for i, frame_obj in enumerate(ann):
                    frame_index = f"{scene_dir}_{i:010d}"  # ✅ 정렬을 위한 키로 사용
                    frame_token = frame_token_map.get(f"{i:010d}")
                    if not frame_token:
                        continue

                    cuboids = frame_obj.get("cuboids", [])
                    for cuboid in cuboids:
                        try:
                            frame_annotation_token = secrets.token_hex(16)
                            uuid = cuboid["uuid"]
                            instance_token = uuid.replace("-", "")

                            annotation_type_name = "bbox_pcd3d"

                            pos = cuboid["position"]
                            dim = cuboid["dimensions"]
                            yaw = cuboid.get("yaw")
                            rotation = yaw_to_quaternion(yaw)

                            size_wlh = json.dumps([dim["x"], dim["y"], dim["z"]])
                            translation_center = json.dumps([pos["x"], pos["y"], pos["z"]])
                            rotation_orientation = json.dumps(rotation)
                            num_lidar_pts = cuboid.get("points_count", 0)

                            label = cuboid.get("label")
                            attributes = cuboid.get("attributes", {})
                            attr_tokens = []
                            for k, v in attributes.items():
                                if k not in ("state", "rider_state"):
                                    continue
                                original_name = f"{label}.{k}.{v}"
                                for name_map in [rename_map_cadcd, rename_map_adds]:
                                    mapped = name_map.get(original_name)
                                    if mapped and mapped in attribute_token_dict:
                                        attr_tokens.append(attribute_token_dict[mapped])

                            uuid_to_annotations[uuid].append({
                                "token": frame_annotation_token,
                                "frame_token": frame_token,
                                "instance_token": instance_token,
                                "annotation_type_name": annotation_type_name,
                                "size_wlh": size_wlh,
                                "translation_center": translation_center,
                                "rotation_orientation": rotation_orientation,
                                "num_lidar_pts": num_lidar_pts,
                                "attribute_tokens": json.dumps(attr_tokens) if attr_tokens else None,
                                "sort_key": frame_index,
                                "label": label,                 # 추가
                                "attributes": attributes        # 추가
                            })

                        except Exception as e:
                            print(f"[경고] frame_annotation 처리 실패: {scene_dir} / frame={i} / {e}")

            # === prev, next 연결 및 frame_annotation 누적 ===
            # uuid가 date 단위로 중복 방생함. date 디렉토리 단위로 누적 후 insert 해야함.
            for uuid, annotations in uuid_to_annotations.items():
                annotations.sort(key=lambda x: x["sort_key"])
                for j, ann in enumerate(annotations):
                    prev_token = annotations[j - 1]["token"] if j > 0 else None
                    next_token = annotations[j + 1]["token"] if j < len(annotations) - 1 else None

                    all_annotation_rows.append((
                        ann["token"],
                        ann["frame_token"],
                        ann["instance_token"],
                        ann["annotation_type_name"],
                        ann["size_wlh"],
                        ann["translation_center"],
                        ann["rotation_orientation"],
                        ann["num_lidar_pts"],
                        ann["attribute_tokens"],
                        prev_token,
                        next_token
                    ))

                # instance 정보 추출
                ann0 = annotations[0]
                label = ann0["label"]
                attributes = ann0["attributes"]

                category_name = None
                if label in label_map_label:
                    category_name = label_map_label[label]
                else:
                    for k, v in attributes.items():
                        if k in ("state", "rider_state"):
                            continue
                        keyval = f"{k}.{v}"
                        if keyval in label_map_attr:
                            category_name = label_map_attr[keyval]
                            break

                category_token = category_token_dict.get(category_name)
                if category_token:
                    instance_token_to_info[uuid] = (
                        category_token,
                        annotations[0]["token"],   # first_frame_annotation_token
                        annotations[-1]["token"]   # last_frame_annotation_token
                    )

            # frame_annotation 테이블 insert
            if all_annotation_rows:
                try:
                    cur.executemany("""
                        INSERT INTO frame_annotation (
                            token, frame_token, instance_token, annotation_type_name,
                            size_wlh, translation_center, rotation_orientation,
                            num_lidar_pts, attribute_tokens, prev, next
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, all_annotation_rows)
                    print(f"[완료] frame_annotation {len(all_annotation_rows)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] frame_annotation insert 실패: {date_dir} / {e}")

            if instance_token_to_info:
                try:
                    cur.executemany("""
                        INSERT INTO instance (
                            token, category_token, num_annotations,
                            first_frame_annotation_token, last_frame_annotation_token
                        ) VALUES (%s, %s, %s, %s, %s)
                    """, [
                        (
                            uuid.replace("-", ""),       # ✅ uuid → instance_token
                            category_token,
                            len(uuid_to_annotations[uuid]),  # ✅ uuid 기준 개수
                            first,
                            last
                        )
                        for uuid, (category_token, first, last) in instance_token_to_info.items()
                    ])
                    print(f"[완료] instance {len(instance_token_to_info)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] instance insert 실패: {date_dir} / {e}")


            # frame 테이블 insert
            if frame_rows:
                query = """
                    INSERT IGNORE INTO frame (
                        token, timestamp, scene_token, prev, next
                    ) VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cur.executemany(query, frame_rows)
                    print(f"[완료] frame 테이블에 {len(frame_rows)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] frame insert 실패: {date_dir} / {e}")

            # log 테이블 insert
            if log_rows:
                query = """
                    INSERT IGNORE INTO log (
                        token, dataset_token, device, date_captured, location
                    ) VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cur.executemany(query, log_rows)
                    print(f"[완료] log 테이블에 {len(log_rows)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] log insert 실패: {date_dir} / {e}")

            # scene 테이블 insert
            if scene_rows:
                query = """
                    INSERT IGNORE INTO scene (
                        token, log_token, base_path, num_frames, first_frame_token, last_frame_token
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                try:
                    cur.executemany(query, scene_rows)
                    print(f"[완료] dataset 테이블에 {len(scene_rows)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] dataset insert 실패: {date_dir} / {e}")
            
            # ego_pose 테이블 insert
            if ego_pose_rows:
                query = """
                    INSERT IGNORE INTO ego_pose (
                        token, timestamp, translation, rotation, pose_type
                    ) VALUES (%s, %s, %s, %s, %s)
                """
                try:
                    cur.executemany(query, ego_pose_rows)
                    print(f"[완료] ego_pose 테이블에 {len(ego_pose_rows)}건 insert 완료: {date_dir}")
                except Exception as e:
                    print(f"[에러] ego_pose insert 실패: {date_dir} / {e}")
                
    conn.commit()
    cur.close()
    conn.close()


def scenarios_in_scene_insert():
    """
    모든 scene에 대해 scenarios_in_scene 테이블에 scene_token, scenario_token 삽입
    scenario 테이블에는 단 하나의 레코드가 있다고 가정
    """
    # MySQL 연결
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE cadcd")

    # scenario 테이블에서 단일 token 가져오기
    cur.execute("SELECT token FROM scenario LIMIT 1")
    result = cur.fetchone()
    if not result:
        print("[에러] scenario 테이블에 레코드가 없습니다. 삽입 중단")
        return

    scenario_token = result[0]
    print(f"[확인] scenario_token: {scenario_token}")

    # scene 테이블에서 모든 token 가져오기
    cur.execute("SELECT token FROM scene")
    scene_tokens = [row[0] for row in cur.fetchall()]

    if not scene_tokens:
        print("[스킵] scene 테이블에 레코드가 없습니다.")
        return

    # 삽입할 데이터 구성
    insert_data = [(scene_token, scenario_token) for scene_token in scene_tokens]

    try:
        cur.executemany(
            "INSERT IGNORE INTO scenarios_in_scene (scene_token, scenario_token) VALUES (%s, %s)",
            insert_data
        )
        conn.commit()
        print(f"[완료] scenarios_in_scene 테이블에 {len(insert_data)}건 insert 완료")
    except Exception as e:
        print(f"[에러] scenarios_in_scene insert 실패: {e}")

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
            db = 'cadcd',
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
        db = 'cadcd',
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
        'attribute_frame_annotation',
        'calibrated_sensor',
        'ego_pose',
        'instance',
        'lidarseg',
        'dataset',
        'scene',
        'log',
        'map',
        'log_map',
        'frame_annotation',
        'frame_data',
        'frame',
        'scenario',
        'scenarios_in_scene',
        'sensor',
        'visibility'
    ]

    for table in tables:
        print(f"Copying table: {table}")
        query = f"""
            INSERT IGNORE INTO adds.{table}
            SELECT * FROM cadcd.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()


with DAG('cadcd_insert',
        default_args = default_args,
        schedule_interval = None,
        )as dag:
    # download_all_ann_files_task = PythonOperator(task_id = 'download_all_ann_files', python_callable = download_all_ann_files)
    # download_all_novatel_files_task = PythonOperator(task_id = 'download_all_novatel_files', python_callable = download_all_novatel_files)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    sensor_insert_task = PythonOperator(task_id = 'sensor_insert', python_callable = sensor_insert)
    integral_insert_task = PythonOperator(task_id = 'integral_insert', python_callable = integral_insert)
    scenario_insert_task = PythonOperator(task_id = 'scenario_insert', python_callable = scenario_insert)
    scenarios_in_scene_insert_task = PythonOperator(task_id = 'scenarios_in_scene_insert', python_callable = scenarios_in_scene_insert)
    
    chunk_params = get_chunk_parameters()

    attribute_frame_annotation_insert_task = PythonOperator.partial(task_id = 'process_batch', python_callable = attribute_frame_annotation_insert).expand(op_kwargs = chunk_params)

    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)
    # cleanup_dirs_task = PythonOperator(task_id='cleanup_dirs', python_callable=cleanup_dirs)


# Define task dependencies # download_all_ann_files_task >> download_all_novatel_files_task >> 
[category_insert_task, attribute_insert_task, sensor_insert_task] >> integral_insert_task >> scenario_insert_task >> scenarios_in_scene_insert_task >> chunk_params >> attribute_frame_annotation_insert_task >> copy_to_adds_task 
