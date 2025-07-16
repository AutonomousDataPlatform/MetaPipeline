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
    local_dir = os.path.join("traffic_light_detection(EV6)", dir_name)
    os.makedirs(local_dir, exist_ok=True)
    return local_dir

# 로컬 디렉토리 전체 삭제
def cleanup_all_local_dirs():
    base_path = "traffic_light_detection(EV6)"
    if os.path.exists(base_path):
        shutil.rmtree(base_path)

def download_all_json_files():
    """
    SFTP에서 /data/traffic_light_detection(EV6)/bbox/scene_*/labels/*/*.json 파일을 다운로드합니다.
    로컬 디렉토리는 traffic_light_detection(EV6)/scene_*/subdir/*.json 형태로 생성됩니다.
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

                    # scene_*/subdir 경로 유지
                    local_dir = prepare_local_dir(f"{outer_dir}/{inner_dir}")
                    local_file_path = os.path.join(local_dir, file_name)
                    remote_file_path = f"{file_path_on_server}/{file_name}"

                    try:
                        sftp_client.get(remote_file_path, local_file_path)
                        print(f"다운로드 완료: {remote_file_path} → {local_file_path}")
                    except Exception as e:
                        print(f"[오류] 다운로드 실패: {remote_file_path} → {e}")


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

    # ✅ scene_*/subdir/*.json 순회
    for scene_dir in os.listdir(base_path):
        scene_path = os.path.join(base_path, scene_dir)
        if not os.path.isdir(scene_path):
            continue

        for subdir in os.listdir(scene_path):
            subdir_path = os.path.join(scene_path, subdir)
            if not os.path.isdir(subdir_path):
                continue

            for file_name in os.listdir(subdir_path):
                if not file_name.endswith(".json"):
                    continue

                file_path = os.path.join(subdir_path, file_name)

                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    annotations = json_data.get("annotations", [])

                    for ann in annotations:
                        attr_str = ann.get("attributes", "")
                        if not attr_str:
                            continue

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
    # category 라벨 맵핑 정의
    label_map = {
        "V-Vehicle Traffic Light": "vehicle_vertical_traffic_light",
        "H-Vehicle Traffic Light": "vehicle_horizontal_traffic_light",
        "Pedestrian Traffic Light": "pedestrian_traffic_light",
    }

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE adds")

    # 기존 category name 목록
    cur.execute("SELECT name FROM category")
    existing_names = {row[0] for row in cur.fetchall()}
    print(f"기존 DB category ::::: {existing_names}")

    name_list = set()
    base_path = "traffic_light_detection(EV6)"

    # ✅ 두 단계 디렉토리 순회
    for scene_dir in os.listdir(base_path):
        scene_path = os.path.join(base_path, scene_dir)
        if not os.path.isdir(scene_path):
            continue

        for subdir in os.listdir(scene_path):
            subdir_path = os.path.join(scene_path, subdir)
            if not os.path.isdir(subdir_path):
                continue

            for file_name in os.listdir(subdir_path):
                if not file_name.endswith(".json"):
                    continue

                file_path = os.path.join(subdir_path, file_name)
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

    # INSERT
    data = [(secrets.token_hex(16), name, None, None) for name in name_list]
    if data:
        query = """
            INSERT IGNORE INTO category (token, name, description, default_scale)
            VALUES (%s, %s, %s, %s)
        """
        cur.executemany(query, data)
        conn.commit()

    cur.close()
    conn.close()

def insert_dummy_log():

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("USE ev6light")  # 작업 대상 DB

    dataset_token = secrets.token_hex(16)
    name = '[자율주행 차량 (EV6)] 신호등 인식 학습 데이터'

    query = "INSERT INTO dataset (token, name) VALUES (%s, %s)"
    cur.execute(query, (dataset_token, name))

    # log 더미 데이터
    log_token = secrets.token_hex(16)
    cur.execute("INSERT IGNORE INTO log (token, dataset_token) VALUES (%s,%s)", (log_token, dataset_token))

    conn.commit()

    cur.close()
    conn.close()


def insert_dummy_sensor_and_calibrated_sensor():
    """
    sensor 및 calibrated_sensor 테이블에 더미 데이터 1개씩 삽입
    sensor → calibrated_sensor 순으로 연결됨
    """

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("USE ev6light")  # 작업 대상 DB

    # sensor 더미 데이터
    sensor_token = secrets.token_hex(16)
    cur.execute("INSERT IGNORE INTO sensor (token) VALUES (%s)", (sensor_token,))
    print(f"sensor insert: {sensor_token}")

    # calibrated_sensor 더미 데이터
    calibrated_sensor_token = secrets.token_hex(16)
    cur.execute(
        "INSERT IGNORE INTO calibrated_sensor (token, sensor_token) VALUES (%s, %s)",
        (calibrated_sensor_token, sensor_token)
    )
    print(f"calibrated_sensor insert: {calibrated_sensor_token}")

    conn.commit()

    cur.close()
    conn.close()


def insert_all_from_directory():
    """
    scene, scenario, instance, frame, frame_data, frame_annotation 통합 insert
    """

    label_map = {
        "V-Vehicle Traffic Light": "vehicle_vertical_traffic_light",
        "H-Vehicle Traffic Light": "vehicle_horizontal_traffic_light",
        "Pedestrian Traffic Light": "pedestrian_traffic_light",
    }

    base_path = "traffic_light_detection(EV6)"

    def convert_bbox_points_to_xyxy(points):
        xs = [pt[0] for pt in points]
        ys = [pt[1] for pt in points]
        return [min(xs), min(ys), max(xs), max(ys)]

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # adds DB에서 category, attribute 조회
    cur.execute("USE adds")
    cur.execute("SELECT name, token FROM category")
    category_dict = {name.strip(): token for name, token in cur.fetchall()}

    cur.execute("SELECT name, token FROM attribute")
    attribute_dict = {name.strip(): token for name, token in cur.fetchall()}

    # 작업 대상 DB로 전환
    cur.execute("USE ev6light")

    # calibrated_sensor token 조회
    cur.execute("SELECT token FROM calibrated_sensor")
    calibrated_sensor_token = cur.fetchone()[0]

    # log token 조회
    cur.execute("SELECT token FROM log")
    log_token = cur.fetchone()[0]

    new_category_list = []

    for scene_dir in os.listdir(base_path):
        scene_path = os.path.join(base_path, scene_dir)
        if not os.path.isdir(scene_path):
            continue

        # scene 디렉토리 단위로 scene 생성
        scene_token = secrets.token_hex(16)
        base_dir_path = f"[자율주행 차량 (EV6)] 신호등 인식 학습 데이터/bbox/{scene_dir}"
        num_frames = None
        first_frame_token = None
        last_frame_token = None

        scene_insert_query = """
            INSERT IGNORE INTO scene
            (token, log_token, base_path, num_frames, first_frame_token, last_frame_token)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(scene_insert_query, (
            scene_token, log_token,
            base_dir_path, num_frames, first_frame_token, last_frame_token
        ))

        # scenario 더미 데이터 생성
        scenario_token = secrets.token_hex(16)

        scenario_insert_query = "INSERT IGNORE INTO scenario (token) VALUES (%s)"
        cur.execute(scenario_insert_query, (scenario_token,))

        # scenario와 scene 맵핑
        scenarios_in_scene_insert_query = "INSERT IGNORE INTO scenarios_in_scene (scene_token, scenario_token) VALUES (%s, %s)"
        cur.execute(scenarios_in_scene_insert_query, (scene_token, scenario_token))

        frame_rows = []
        frame_data_rows = []
        instance_rows = []
        frame_annotation_rows = []

        for subdir in os.listdir(scene_path):
            subdir_path = os.path.join(scene_path, subdir)
            if not os.path.isdir(subdir_path):
                continue

            for file_name in os.listdir(subdir_path):
                if not file_name.endswith(".json"):
                    continue

                file_path = os.path.join(subdir_path, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        sample_json = json.load(f)

                    annotations = sample_json.get("annotations", [])
                    metadata = sample_json.get("metadata", {})
                    raw_filename = sample_json.get("filename", "").strip()

                    image_file_path = f"images/{subdir}/{raw_filename}"
                    parts = file_name.rstrip(".json").split("_")
                    seconds = parts[1]
                    micro = parts[2][:6]
                    timestamp = f"{seconds}.{micro}"

                    frame_token = secrets.token_hex(16)
                    frame_data_token = secrets.token_hex(16)

                    frame_rows.append((frame_token, scene_token))

                    frame_data_rows.append((
                        frame_data_token, frame_token, None, calibrated_sensor_token,
                        timestamp, None, metadata.get("height", 0), metadata.get("width", 0),
                        image_file_path, "jpg", None, None
                    ))

                    for ann in annotations:
                        instance_token = secrets.token_hex(16)
                        raw_label = ann.get("label", "").strip()
                        mapped_label = label_map.get(raw_label, raw_label)
                        category_token = category_dict.get(mapped_label)

                        if category_token is None:
                            category_token = secrets.token_hex(16)
                            category_dict[mapped_label] = category_token
                            new_category_list.append((category_token, mapped_label))

                        instance_rows.append((
                            instance_token, category_token, None, None, None
                        ))

                        attr_str = ann.get("attributes", "")
                        points = ann.get("points", [])
                        bbox = convert_bbox_points_to_xyxy(points)
                        bbox_json = json.dumps({"bbox": bbox})

                        frame_annotation_token = secrets.token_hex(16)
                        attr_token = attribute_dict.get(attr_str)
                        attribute_tokens = json.dumps([attr_token]) if attr_token else None

                        frame_annotation_rows.append((
                            frame_annotation_token, frame_token, frame_data_token, instance_token,
                            None, "bbox_image2d", None, None, None, None, None,
                            bbox_json, None, None, attribute_tokens,
                            None, None, None, None
                        ))

                except Exception as e:
                    print(f"[실패] {file_path}: {e}")
                    continue

        if frame_rows:
            cur.executemany("INSERT IGNORE INTO frame (token, scene_token) VALUES (%s, %s)", frame_rows)

        if frame_data_rows:
            cur.executemany("""
                INSERT IGNORE INTO frame_data (
                    token, frame_token, ego_pose_token, calibrated_sensor_token,
                    timestamp, is_key_frame, height, width,
                    file_name, file_format, prev, next
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, frame_data_rows)

        if instance_rows:
            cur.executemany("""
                INSERT IGNORE INTO instance (
                    token, category_token, first_frame_annotation_token,
                    last_frame_annotation_token, num_annotations
                ) VALUES (%s,%s,%s,%s,%s)
            """, instance_rows)
            
        if frame_annotation_rows:
            cur.executemany("""
                INSERT IGNORE INTO frame_annotation (
                    token, frame_token, frame_data_token, instance_token,
                    visibility_token, annotation_type_name, is_lidar_synced,
                    size_wlh, translation_center, rotation_orientation, bbox_image3d,
                    bbox_image2d, num_lidar_pts, num_radar_pts, attribute_tokens,
                    frame_aligned, description, prev, next
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, frame_annotation_rows)

        conn.commit()
        print(f"{scene_dir}: {len(frame_data_rows)} frame_data, {len(instance_rows)} instance, {len(frame_annotation_rows)} annotations inserted.")

    if new_category_list:
        cur.executemany(
            "INSERT IGNORE INTO adds.category (token, name) VALUES (%s, %s)",
            new_category_list
        )
        conn.commit()
        print(f"[추가] adds.category에 {len(new_category_list)}개의 새 category를 삽입했습니다.")

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
            db = 'ev6light',
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
        db = 'ev6light',
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
    #download_all_json_files_task = PythonOperator(task_id = 'download_all_json_files', python_callable = download_all_json_files)

    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    insert_dummy_log_task = PythonOperator(task_id = 'insert_dummy_log', python_callable = insert_dummy_log)
    insert_dummy_sensor_and_calibrated_sensor_task = PythonOperator(task_id = 'insert_dummy_sensor_and_calibrated_sensor', python_callable = insert_dummy_sensor_and_calibrated_sensor)
    insert_all_from_directory_task = PythonOperator(task_id = 'insert_all_from_directory', python_callable = insert_all_from_directory)

    chunk_params = get_chunk_parameters()

    attribute_frame_annotation_insert_task = PythonOperator.partial(task_id = 'process_batch', python_callable = attribute_frame_annotation_insert).expand(op_kwargs = chunk_params)
    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)

[attribute_insert_task, category_insert_task, insert_dummy_log_task, insert_dummy_sensor_and_calibrated_sensor_task] >> insert_all_from_directory_task >> chunk_params >> attribute_frame_annotation_insert_task >> copy_to_adds_task