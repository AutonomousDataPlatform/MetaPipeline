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


default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes = 5),
}


kakao_remote_path = '/data/traffic_light_detection(EV6)/bbox'

def prepare_local_dir(dir_name):
    local_dir = f"traffic_light_detection(EV6)/{dir_name}"
    os.makedirs(local_dir, exist_ok=True)
    return local_dir

def cleanup_dirs():
    base_path = "tail_light_detection(autonomous_vehicle)"
    if os.path.exists(base_path):
        for dir_name in os.listdir(base_path):
            dir_path = os.path.join(base_path, dir_name)
            if os.path.isdir(dir_path):
                shutil.rmtree(dir_path)


def download_all_json_files():
    """
    SFTP에서 /data/traffic_light_detection(EV6)/bbox/scene_*/labels/*/*.json 파일을 전부 다운로드합니다.
    로컬 디렉토리는 prepare_local_dir(f"{outer_dir}_{inner_dir}") 형태로 생성됩니다.
    """
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_outer_dirs = hook.list_directory(kakao_remote_path)

    with hook.get_conn() as sftp_client:
        for outer_dir in kakao_outer_dirs:
            label_base_path = f"{kakao_remote_path}/{outer_dir}/labels"

            try:
                label_subdirs = hook.list_directory(label_base_path)
            except Exception as e:
                print(f"[경고] labels 폴더 없음: {label_base_path} → {e}")
                continue

            for inner_dir in label_subdirs:
                file_path_on_server = f"{label_base_path}/{inner_dir}"
                try:
                    file_list = hook.list_directory(file_path_on_server)
                except Exception as e:
                    print(f"[경고] 하위 디렉토리 접근 실패: {file_path_on_server} → {e}")
                    continue

                for file_name in file_list:
                    if not file_name.endswith(".json"):
                        continue

                    local_dir = prepare_local_dir(f"{outer_dir}_{inner_dir}")
                    local_file_path = f"{local_dir}/{file_name}"
                    remote_file_path = f"{file_path_on_server}/{file_name}"

                    try:
                        sftp_client.get(remote_file_path, local_file_path)
                        print(f"다운로드 완료: {remote_file_path} → {local_file_path}")
                    except Exception as e:
                        print(f"[오류] 다운로드 실패: {remote_file_path} → {e}")


def dataset_insert():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_outer_dirs = hook.list_directory(kakao_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE ev6light")

    with hook.get_conn() as sftp_client:
        for outer_dir in kakao_outer_dirs:
            label_base_path = f"{kakao_remote_path}/{outer_dir}/labels"

            try:
                label_subdirs = hook.list_directory(label_base_path)
            except Exception as e:
                print(f"[경고] labels 폴더가 없음: {label_base_path}, 에러: {e}")
                continue

            for inner_dir in label_subdirs:
                file_path_on_server = f"{label_base_path}/{inner_dir}"
                try:
                    file_list = hook.list_directory(file_path_on_server)
                except Exception as e:
                    print(f"[경고] 서브디렉토리 접근 실패: {file_path_on_server}, 에러: {e}")
                    continue

                for file_name in file_list:
                    if not file_name.endswith(".json"):
                        continue

                    local_dir = prepare_local_dir(f"{outer_dir}_{inner_dir}")
                    local_file_path = f"{local_dir}/{file_name}"
                    remote_file_path = f"{file_path_on_server}/{file_name}"

                    sftp_client.get(remote_file_path, local_file_path)

                    with open(local_file_path) as f:
                        json_data = json.load(f)

                        # 추출 로직은 예시. 구조를 정확히 알면 수정 필요
                        uuid = json_data.get('uuid') or f"{outer_dir}_{inner_dir}_{file_name}"
                        token = uuid.replace("-", "")[:32]

                        log_uuid = json_data.get('log_uuid', 'dummy-log')
                        log_token = log_uuid.replace("-", "")[:32]

                        base_path = file_path_on_server
                        num_frames = 1  # frame 단위 파일이므로
                        first_frame_token = token
                        last_frame_token = token

                        data = (
                            token, "kakao_mobility", log_token,
                            base_path, num_frames, first_frame_token, last_frame_token
                        )

                        query = """
                            INSERT IGNORE INTO dataset
                            (token, type, log_token, base_path, num_frames, first_frame_token, last_frame_token)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """
                        cur.execute(query, data)
                        conn.commit()

    cur.close()
    conn.close()


def dataset_insert():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    kakao_outer_dirs = hook.list_directory(kakao_remote_path)

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE ev6light")

    with hook.get_conn() as sftp_client:
        for outer_dir in kakao_outer_dirs:

            num_frames = None

            # UUID 없이 outer_dir 자체를 기반으로 token 구성
            token = secrets.token_hex(16)
            log_token = None
            base_path = f"{kakao_remote_path}/{outer_dir}"
            first_frame_token = None
            last_frame_token = None

            data = (
                token, "kakao_mobility", log_token,
                base_path, num_frames, first_frame_token, last_frame_token
            )

            query = """
                INSERT IGNORE INTO dataset
                (token, type, log_token, base_path, num_frames, first_frame_token, last_frame_token)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(query, data)
            conn.commit()

    cur.close()
    conn.close()


def attribute_insert():

    # MySQL 접속
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE adds")

    # 기존 attribute.name 조회
    cur.execute("SELECT name FROM attribute")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB의 attribute ::::: {existing_names}")

    # 최종 insert 대상 name 저장용
    name_list = set()

    # 로컬 디렉토리 경로
    base_path = "traffic_light_detection(EV6)"

    for scene_dir in os.listdir(base_path):
        scene_path = os.path.join(base_path, scene_dir)
        if not os.path.isdir(scene_path):
            continue

        for file_name in os.listdir(scene_path):
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(scene_path, file_name)

            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                annotations = json_data.get("annotations", [])

                for ann in annotations:
                    attr_str = ann.get("attributes", "")
                    if not attr_str:
                        continue

                    # 기존 DB에 없으면만 추가
                    if attr_str not in existing_names:
                        name_list.add(attr_str)
                        existing_names.add(attr_str)

    print(f"기존 attribute 테이블에 없는 데이터 ::::: {name_list}")

    # INSERT 수행
    data = [(secrets.token_hex(16), name) for name in name_list]
    insert_query = "INSERT IGNORE INTO attribute (token, name) VALUES (%s, %s)"
    cur.executemany(insert_query, data)
    conn.commit()

    cur.close()
    conn.close()


def category_insert():

    # category 라벨 맵핑 정의 (ev6light → avlight 명칭)
    label_map = {
        "V-Vehicle Traffic Light": "vehicle_vertical_traffic_light",
        "H-Vehicle Traffic Light": "vehicle_horizontal_traffic_light",
        "Pedestrian Traffic Light": "pedestrian_traffic_light",
    }

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE adds")

    # 이미 존재하는 category.name 조회
    cur.execute("SELECT name FROM category")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB category ::::: {existing_names}")

    # 중복 제거된 삽입 대상 set
    name_list = set()

    # 로컬 base 디렉토리
    base_path = "traffic_light_detection(EV6)"

    for scene_dir in os.listdir(base_path):
        scene_path = os.path.join(base_path, scene_dir)
        if not os.path.isdir(scene_path):
            continue

        for file_name in os.listdir(scene_path):
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(scene_path, file_name)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    annotations = json_data.get("annotations", [])

                    for ann in annotations:
                        raw_label = ann.get("label", "").strip()
                        mapped_label = label_map.get(raw_label, raw_label)
                        if mapped_label and mapped_label not in existing_names:
                            name_list.add(mapped_label)
                            existing_names.add(mapped_label)

            except json.JSONDecodeError as e:
                print(f"[경고] JSON 파싱 실패: {file_path} / {e}")
                continue

    print(f"최종 insert 대상 category ::::: {name_list}")

    # insert 대상 준비
    data = [(secrets.token_hex(16), name, None, None) for name in name_list]

    if data:
        print(f"최종 insert 대상 category ::::: {name_list}")
        query = """
            INSERT IGNORE INTO category (token, name, description, default_scale)
            VALUES (%s, %s, %s, %s)
        """
        cur.executemany(query, data)
        conn.commit()

    cur.close()
    conn.close()


def insert_all_from_directory():
    """
    디렉토리 하나마다 frame_data는 항상 insert하고,
    annotations가 있는 경우에만 instance 및 frame_annotation insert.
    """

    # ev6light → avlight 명칭 맵핑
    label_map = {
        "V-Vehicle Traffic Light": "vehicle_vertical_traffic_light",
        "H-Vehicle Traffic Light": "vehicle_horizontal_traffic_light",
        "Pedestrian Traffic Light": "pedestrian_traffic_light",
    }

    # 로컬 저장 경로 기준
    base_path = "traffic_light_detection(EV6)"

    # bbox 좌표를 xmin,ymin,xmax,ymax 로 변환
    def convert_bbox_points_to_xyxy(points):
        xs = [pt[0] for pt in points]
        ys = [pt[1] for pt in points]
        return [min(xs), min(ys), max(xs), max(ys)]

    # MySQL 연결
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # adds DB에서 category, attribute 가져오기
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM category")
    category_dict = {name.strip(): token for name, token in cur.fetchall()}

    cur.execute("SELECT name, token FROM attribute")
    attribute_dict = {name.strip(): token for name, token in cur.fetchall()}

    # 작업 대상 DB로 복귀
    cur.execute("USE ev6light")

    new_category_list = [] # 못 찾은 category를 모아둘 리스트

    print(f"기존 DB category 데이터 ::::::: {category_dict}")

    # scene_xxx_FF, scene_xxx_RR 등 디렉토리 순회
    for dir_name in os.listdir(base_path):
        dir_path = os.path.join(base_path, dir_name)

        if not os.path.isdir(dir_path):
            continue

        # outer_dir, inner_dir 분리: 예 "scene_20240521_154713_FF" → "scene_20240521_154713", "FF"
        if "_" not in dir_name:
            continue

        try:
            outer_dir, inner_dir = dir_name.rsplit("_", 1)
        except ValueError:
            continue

        frame_data_rows = []
        instance_rows = []
        frame_annotation_rows = []

        # 해당 디렉토리 내 .json 파일 순회
        for file_name in os.listdir(dir_path):
            if not file_name.endswith(".json"):
                continue

            file_path = os.path.join(dir_path, file_name)

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    sample_json = json.load(f)

                annotations = sample_json.get("annotations", [])
                metadata = sample_json.get("metadata", {})
                raw_filename = sample_json.get("filename", "").strip()

                # 이미지 파일 경로 (frame_data_insert와 동일 방식)
                image_file_path = f"images/{inner_dir}/{raw_filename}"

                # timestamp 추출 (파일명 ex: 1_1716274034_137845000.json → "1716274034.137845")
                parts = file_name.rstrip(".json").split("_")
                seconds = parts[1]
                micro = parts[2][:6]
                timestamp = f"{seconds}.{micro}"

                # frame_data 데이터 생성
                frame_data_token = secrets.token_hex(16)
                frame_token = None
                ego_pose_token = None
                calibrated_sensor_token = None
                is_key_frame = None
                height = metadata.get("height", 0)
                width = metadata.get("width", 0)
                file_format = "jpg"
                prev = None
                next = None

                frame_data_rows.append((
                    frame_data_token, frame_token, ego_pose_token, calibrated_sensor_token,
                    timestamp, is_key_frame, height, width,
                    image_file_path, file_format, prev, next
                ))

                if annotations:
                    for ann in annotations:

                        # instance 데이터 생성
                        instance_token = secrets.token_hex(16)
                        raw_label = ann.get("label", "").strip()
                        mapped_label = label_map.get(raw_label, raw_label)
                        category_token = category_dict.get(mapped_label)

                        # category_token이 없으면 후처리용으로 생성하고 누적
                        if category_token is None:
                            category_token = secrets.token_hex(16)
                            category_dict[mapped_label] = category_token
                            new_category_list.append((category_token, mapped_label))

                        first_frame_annotation_token, last_frame_annotation_token = None, None
                        num_annotations = None

                        instance_rows.append((
                            instance_token, category_token,
                            first_frame_annotation_token, last_frame_annotation_token, num_annotations
                        ))

                        # frame_annotation 데이터 생성
                        attr_str = ann.get("attributes", "")
                        points = ann.get("points", [])

                        bbox = convert_bbox_points_to_xyxy(points)
                        bbox_json = json.dumps({"bbox": bbox})

                        frame_annotation_token = secrets.token_hex(16)
                        visibility_token = None
                        annotation_type_name = "bbox_image2d"
                        is_lidar_synced = None
                        size_wlh, translation_center, rotation_orientation = None, None, None
                        bbox_image3d = None
                        bbox_image2d = bbox_json
                        num_lidar_pts, num_radar_pts = None, None
                        attr_token = attribute_dict.get(attr_str)
                        attribute_tokens = json.dumps([attr_token]) if attr_token else None
                        frame_aligned, description, prev, next = None, None, None, None

                        frame_annotation_rows.append((
                            frame_annotation_token, frame_token, frame_data_token, instance_token,
                            visibility_token, annotation_type_name, is_lidar_synced,
                            size_wlh, translation_center, rotation_orientation, bbox_image3d,
                            bbox_image2d, num_lidar_pts, num_radar_pts, attribute_tokens,
                            frame_aligned, description, prev, next
                        ))

            except Exception as e:
                print(f"[실패] {file_path}: {e}")
                continue

        # 데이터 INSERT 실행 및 commit (디렉토리 단위)
        frame_data_query = """
            INSERT IGNORE INTO frame_data (
                token, frame_token, ego_pose_token, calibrated_sensor_token,
                timestamp, is_key_frame, height, width,
                file_name, file_format, prev, next
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        instance_query = """
            INSERT IGNORE INTO instance (
                token, category_token, first_frame_annotation_token,
                last_frame_annotation_token, num_annotations
            ) VALUES (%s,%s,%s,%s,%s)
        """

        frame_annotation_query = """
            INSERT IGNORE INTO frame_annotation (
                token, frame_token, frame_data_token, instance_token,
                visibility_token, annotation_type_name, is_lidar_synced,
                size_wlh, translation_center, rotation_orientation, bbox_image3d,
                bbox_image2d, num_lidar_pts, num_radar_pts, attribute_tokens,
                frame_aligned, description, prev, next
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        if frame_data_rows:
            cur.executemany(frame_data_query, frame_data_rows)
        if instance_rows:
            cur.executemany(instance_query, instance_rows)
        if frame_annotation_rows:
            cur.executemany(frame_annotation_query, frame_annotation_rows)

        conn.commit()
        print(f"{dir_name}: {len(frame_data_rows)} frame_data, {len(instance_rows)} instance, {len(frame_annotation_rows)} annotations inserted.")
    
    print(f"기존 category에 없는 데이터 ::::::: {new_category_list}")

    # adds.category에 새 category 한 번에 insert
    if new_category_list:
        cur.executemany(
            "INSERT IGNORE INTO adds.category (token, name) VALUES (%s, %s)",
            new_category_list
        )
        conn.commit()
        print(f"[추가] adds.category에 {len(new_category_list)}개의 새 category를 삽입했습니다.")
    
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
            SELECT * FROM ev6light.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()


with DAG('traffic_light_detection_EV6_insert',
        default_args = default_args,
        schedule_interval = None,
        )as dag:
    
    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    insert_all_from_directory_task = PythonOperator(task_id = 'insert_all_from_directory', python_callable = insert_all_from_directory)
    dataset_insert_task = PythonOperator(task_id = 'dataset_insert', python_callable = dataset_insert)
    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)

[attribute_insert_task, category_insert_task, dataset_insert_task] >> insert_all_from_directory_task >> copy_to_adds_task