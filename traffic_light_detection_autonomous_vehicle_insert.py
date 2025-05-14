from datetime import timedelta
from datetime import datetime
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

default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes = 5),
}

# NOTE # FIXME: [자율주행차량]신호등 데이터셋은 하나의 데이터셋가지고 모든 데이터 넣는거니까, 처음 한번 json 파일 가져오고, 마지막에 삭제하면 됨. task마다 똑같은 파일 다운했다가 삭제했다가 할 필요 없음.

# 공통 경로
dataset_dir = "traffic_light_detection(autonomous_vehicle)"
kakao_remote_path = '/data/traffic_light_detection(autonomous_vehicle)'

def prepare_local_dir(dir_name):
    local_dir = f"traffic_light_detection(autonomous_vehicle)/{dir_name}/annotations"
    os.makedirs(local_dir, exist_ok=True)
    return local_dir

# SFTP에서 모든 instances.json 다운로드
def download_all_instances_json():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    for dir_name in kakao_directory_list:
        local_dir = prepare_local_dir(dir_name, create=True)
        remote_path = f"{kakao_remote_path}/{dir_name}/annotations/instances.json"
        local_path = f"{local_dir}/instances.json"

        with hook.get_conn() as sftp_client:
            sftp_client.get(remote_path, local_path)

# 로컬 디렉토리 전체 삭제
def cleanup_all_local_dirs():
    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)



def attribute_insert():

    # sftp 접속
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    # mysql 접속
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE adds")

    # 기존 DB의 attribute.name 불러오기
    cur.execute("SELECT name FROM attribute")
    existing_names = {row[0] for row in cur.fetchall()}

    print(f"기존 DB에 있는 attribute :::::: {existing_names}")

    # description 매핑 테이블
    bulb_description_map = {
        "R": "{bulb} is Red",
        "Y": "{bulb} is Yellow",
        "G": "{bulb} is Green",
        "off": "{bulb} is off (lamp exists but not lit)",
        "X": "{bulb} is not present (no lamp installed)",
        "PR": "{bulb} is Pedestrian Red",
        "PG": "{bulb} is Pedestrian Green",
        "BR": "{bulb} is Bus Red",
        "BG": "{bulb} is Bus Green",
        "0": "{bulb} is cross (X) direction",
        "1": "{bulb} is bottom-left direction",
        "2": "{bulb} is straight (down) direction",
        "3": "{bulb} direction is unknown (unconfirmed)",
        "4": "{bulb} is left direction",
        "5": "{bulb} direction is unknown (unconfirmed)",
        "6": "{bulb} is right direction",
        "7": "{bulb} is top-left direction",
        "8": "{bulb} is upward direction",
        "9": "{bulb} is top-right direction"
    }

    # name + description 저장
    attribute_dict = {}

    for dir_name in kakao_directory_list:
        local_dir = prepare_local_dir(dir_name)
        local_path = f"{local_dir}/instances.json"

        with open(local_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

            # instance.json => annotations 필드에 attributes 데이터 존재
            for annotation in json_data.get("annotations", []):
                attr_dict = annotation.get("attributes", {})

                for key, val in attr_dict.items():
                    if key.startswith("Bulb") and isinstance(val, (str, int)):

                        # '_state'가 있으면 제거
                        if '_state' in key:
                            # '_state'를 제거
                            base_key = key.replace("_state", "")
                        else:
                            # '_state'가 없으면 그대로 사용
                            base_key = key

                        attr_name = f"{base_key}.{val}"

                        # 기존에 없는 경우만 추가
                        if attr_name not in existing_names:
                            desc_template = bulb_description_map.get(str(val))
                            if desc_template:
                                description = desc_template.format(bulb = base_key)
                            else:
                                description = f"{base_key} has unknown state"

                            attribute_dict[attr_name] = description
                            existing_names.add(attr_name)

    print(f"기존 attribute 테이블에 없는 데이터 ::::: {attribute_dict}")

    # insert할 데이터 준비
    data = [
        (secrets.token_hex(16), name, desc)
        for name, desc in attribute_dict.items()
    ]

    query = "INSERT IGNORE INTO attribute (token, name, description) VALUES (%s, %s, %s)"
    cur.executemany(query, data)
    conn.commit()

    cur.close()
    conn.close()


def category_insert(**context):

    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list = hook.list_directory(kakao_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE avlight")

    # 이미 존재하는 category.name 조회
    cur.execute("SELECT name FROM category")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB에 있는 category ::::: {existing_names}")

    # insert 대상 및 token 매핑용 딕셔너리 준비
    name_list = []
    category_id_to_token = {}

    kor_to_eng = {
        "가변형 가변등": "variable_variable_light",
        "차량용신호등": "vehicle_traffic_light",
        "보행자신호등": "pedestrian_traffic_light",
        "자전거종형": "bicycle_vertical_traffic_light",
        "버스삼색등": "bus_three_color_traffic_light",
        "기타신호등": "other_traffic_light",
        "경보형 가변등": "warning_variable_traffic_light",
        "차량종형": "vehicle_vertical_traffic_light",
        "차량횡형": "vehicle_horizontal_traffic_light",
    }

    for dir_name in kakao_directory_list:
        local_dir = prepare_local_dir(dir_name)
        local_path = f"{local_dir}/instances.json"

        with open(local_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            categories = json_data.get('categories', [])

            for category in categories:
                kor_name = category.get('name')
                category_id = category.get('id')
                eng_name = kor_to_eng.get(kor_name, "unknown_category")
                description = kor_name
                default_scale = None

                # 중복 방지
                if eng_name not in existing_names:
                    token = secrets.token_hex(16)
                    name_list.append((token, eng_name, description, default_scale))
                    existing_names.add(eng_name)

                    # token 매핑 (XCom 용)
                    if category_id is not None:
                        category_id_to_token[str(category_id)] = token
        
    print(f"최종 insert 대상 category ::::: {name_list}")

    # insert 수행
    if name_list:
        print(f"최종 insert 대상 category ::::: {name_list}")
        query = "INSERT IGNORE INTO category (token, name, description, default_scale) VALUES (%s, %s, %s, %s)"
        cur.executemany(query, name_list)
        conn.commit()

    cur.close()
    conn.close()

    # XCom push
    context['ti'].xcom_push(key='category_id_to_token', value=category_id_to_token)


def log_and_dataset_insert():

    # SFTP, MySQL 연결
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE avlight")

    log_token_dict = {}

    kakao_directory_list = hook.list_directory(kakao_remote_path)

    for dir_name in kakao_directory_list:
        local_dir = prepare_local_dir(dir_name)
        local_path = f"{local_dir}/instances.json"

        try:
            with open(local_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"[WARN] JSON 로딩 실패: {local_path} → {e}")
            continue

        info = json_data.get("info")
        if not info or not info.get("date_created"):
            print(f"[WARN] info 또는 date_created 누락 → {dir_name} 건너뜀")
            continue

        # log 테이블 insert
        log_token = secrets.token_hex(16)
        dt_obj = datetime.strptime(info["date_created"], "%Y/%m/%d")
        date_captured = dt_obj.strftime("%Y-%m-%d")
        cur.execute("INSERT IGNORE INTO log (token, date_captured) VALUES (%s, %s)", (log_token, date_captured))

        # dataset 테이블 insert
        dataset_token = secrets.token_hex(16)
        dataset_type = "kakao_mobility"
        base_path = f"traffic_light_detection(autonomous_vehicle)/{dir_name}"
        num_frames = None
        first_frame_token = None
        last_frame_token = None
        cur.execute("""
            INSERT IGNORE INTO dataset (token, type, log_token, base_path, num_frames, first_frame_token, last_frame_token)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (dataset_token, dataset_type, log_token, base_path, num_frames, first_frame_token, last_frame_token))

        conn.commit()
        log_token_dict[dir_name] = log_token
        

    cur.close()
    conn.close()


def insert_all_from_instances_json(**context):
    """
    instances.json 기반으로 frame_data, instance, frame_annotation 통합 insert
    """

    # Airflow 연결
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

     # 1. 통합 DB에서 attribute 조회
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM attribute")
    attribute_result = cur.fetchall()
    attribute_name_to_token = {row[0]: row[1] for row in attribute_result}

     # 2. 작업 대상 DB로 전환
    cur.execute("USE avlight")

    # category_id_to_token: XCom으로 받음
    category_id_to_token = context['ti'].xcom_pull(key='category_id_to_token', task_ids='category_insert')
    print(f"[INFO] category_id_to_token ::::: {category_id_to_token}")

    frame_data_rows = []
    instance_rows = []
    frame_annotation_rows = []

    kakao_directory_list = hook.list_directory(kakao_remote_path)

    for dir_name in kakao_directory_list:
        local_dir = prepare_local_dir(dir_name)
        local_path = f"{local_dir}/instances.json"

        # json 파일 열기
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"[WARN] JSON 파싱 실패: {local_path} → {e}")
            continue

        image_id_to_token = {}

        # 1. frame_data 처리
        for img in json_data.get("images", []):
            image_id = str(img["id"])
            
            token = secrets.token_hex(16)
            width = img['width']
            height = img['height']
            file_name = img['file_name']

            image_id_to_token[image_id] = token    # image_id → token 매핑 저장

            # 파일 확장자 추출
            _, ext = os.path.splitext(file_name)
            file_format = ext.lstrip(".")   # .bmp -> bmp

            frame_data_rows.append((token, height, width, file_name, file_format))

        # 2. instance & frame_annotation 처리
        for ann in json_data.get("annotations", []):
            annotation_id = ann["id"]
            image_id = str(ann["image_id"])
            category_id = str(ann["category_id"])
            frame_data_token = image_id_to_token.get(image_id)

            if not frame_data_token:
                print(f"[WARN] image_id {image_id}에 해당하는 frame_data_token 없음")
                continue

            category_token = category_id_to_token.get(category_id)
            if not category_token:
                print(f"[WARN] category_id {category_id}에 해당하는 token 없음")
                continue
            
            # instance 테이블
            instance_token = secrets.token_hex(16)
            first_frame_annotation_token = None
            last_frame_annotation_token = None
            num_annotations = 1

            instance_rows.append((instance_token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations))

            frame_annotation_token = secrets.token_hex(16)

            # occluded 가 false면 가려지지 않았다 => 즉 visibility 가 100% 이다.
            occluded = ann["attributes"].get("occluded", False)
            visibility_token = "0" if occluded else "5"

            annotation_type_name = "bbox_image2d"

            attribute_tokens = []
            for key, value in ann["attributes"].items():
                if key in ["occluded", "rotation"]:
                    continue
                attribute_name = f"{key}.{value}"
                attribute_token = attribute_name_to_token.get(attribute_name)
                if attribute_token:
                    attribute_tokens.append(attribute_token)

            attribute_tokens_json = json.dumps(attribute_tokens)

            # bbox_image2d 좌표
            bbox = ann["bbox"]
            x_min = bbox[0]
            y_min = bbox[1]
            x_max = bbox[0] + bbox[2]
            y_max = bbox[1] + bbox[3]
            bbox_image2d = json.dumps([x_min, y_min, x_max, y_max])

            frame_annotation_rows.append((
                frame_annotation_token,
                frame_data_token,
                instance_token,
                visibility_token,
                annotation_type_name,
                attribute_tokens_json,
                bbox_image2d
            ))
        

    # frame_data, instance, frame_annotation INSERT
    cur.executemany(
        "INSERT IGNORE INTO frame_data (token, height, width, file_name, file_format) VALUES (%s, %s, %s, %s, %s)",
        frame_data_rows
    )
    cur.executemany(
        "INSERT IGNORE INTO instance (token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations) VALUES (%s, %s, %s, %s, %s)",
        instance_rows
    )
    cur.executemany(
        "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, visibility_token, annotation_type_name, attribute_tokens, bbox_image2d) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        frame_annotation_rows
    )
    conn.commit()

    print(f"✅ Inserted: {len(frame_data_rows)} frame_data, {len(instance_rows)} instances, {len(frame_annotation_rows)} frame_annotations")

    cur.close()
    conn.close()


def copy_to_adds():
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    tables = [
        'annotation_type',
        'attribute',
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
            SELECT * FROM avlight.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()


with DAG('traffic_light_detection_autonomous_vehicle_insert',
        default_args = default_args,
        schedule_interval = None,
        )as dag:

    #download_task = PythonOperator(task_id = 'download_all_instances_json', python_callable = download_all_instances_json)
    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    log_and_dataset_insert_task = PythonOperator(task_id = 'log_and_dataset_insert', python_callable = log_and_dataset_insert)
    insert_all_from_instances_json_task = PythonOperator(task_id = 'insert_all_from_instances_json', python_callable = insert_all_from_instances_json)
    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)
    #cleanup_task = PythonOperator(task_id = 'cleanup_all_local_dirs',python_callable = cleanup_all_local_dirs)

attribute_insert_task >> category_insert_task >> [log_and_dataset_insert_task, insert_all_from_instances_json_task] >> copy_to_adds_task

# download_task >> attribute_insert_task >> category_insert_task >> [log_and_dataset_insert_task, insert_all_from_instances_json_task] >> copy_to_adds_task >> cleanup_task