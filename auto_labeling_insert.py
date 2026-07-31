"""
auto_labeling_insert DAG

KITTI auto-labeling API 결과(`output/{JOB_UUID}/*.json`) 를 읽어
스테이징 DB(`auto_label`) 에 적재한 뒤 통합 DB(`adds`) 로 복사한다.

테이블 분류:
  - 공유 마스터 (dataset / sensor / category / annotation_type)
      → adds 에 직접 조회·삽입. 있으면 기존 token 재사용, staging 을 거치지 않는다.
  - 업로드(job) 단위 (log / scene / calibrated_sensor / frame / frame_data /
                      scenario / scenarios_in_scene / instance / frame_annotation_new)
      → staging 에 적재 후 copy_to_adds 가 이번 job 것만 adds 로 복사.

모든 token 은 자연키 기반 결정적 값(stable_token)이라 DAG 재실행 시 중복이 생기지
않는다. 단, 같은 ZIP 을 다시 업로드하면 job_id 가 달라져 별개 데이터로 적재된다.

트리거 예:
    {"job_id": "7a34431e-8d99-4043-ba68-0f1a0f97cdc8"}

상세 설계는 auto-labeling-api/docs/etl_mapping_transformation_rules.md 참조.
"""

from datetime import datetime, timedelta, timezone
import glob
import hashlib
import json
import os
import uuid

import numpy as np
from scipy.spatial.transform import Rotation

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


KST = timezone(timedelta(hours=9))

default_args = {
    'owner': 'eun',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# --- 상수 -----------------------------------------------------------------

OUTPUT_ROOT = "/home/ubuntu/auto-labeling-api/output"
STAGING_DB = "auto_label"
ADDS_DB = "adds"
PROVENANCE_DB = "adds_provenance"
MYSQL_CONN_ID = "aws_mysql"

DATASET_NAME = "KITTI Auto-Labeled"
SENSOR_CHANNEL = "AUTO_LABEL_CAM"
SENSOR_MODALITY = "camera"
ANNOTATION_TYPE_NAME = "bbox_image2d"
ANNOTATION_TYPE_DESC = "2D bounding box from KITTI auto-labeling"


# --- 공통 헬퍼 ------------------------------------------------------------

def stable_token(*parts):
    """
    자연키로부터 결정적 token(char(32)) 생성.

    같은 입력이면 항상 같은 값이 나오므로 INSERT IGNORE 가 실제로 중복을 걸러낸다.
    (랜덤 토큰은 절대 충돌하지 않아 IGNORE 가 무력해지고, DAG 재실행 시
     scene 을 제외한 모든 테이블이 2배로 늘어난다.)
    """
    key = "|".join(str(p) for p in parts)
    return hashlib.md5(key.encode('utf-8')).hexdigest()


def get_job_id(kwargs):
    """DAG 트리거 conf 로 전달된 job_id 반환."""
    conf = kwargs['dag_run'].conf or {}
    job_id = conf.get('job_id')
    if not job_id:
        raise ValueError("dag_run.conf['job_id'] 가 필요합니다.")
    return job_id


def list_json_files(job_id):
    """해당 job 디렉토리의 JSON 파일을 파일명 오름차순으로 반환."""
    pattern = os.path.join(OUTPUT_ROOT, job_id, "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"JSON 파일이 없습니다: {pattern}")
    return files


def _find_calib_line(original_calib, key):
    """찾으면 값 문자열, 못 찾으면 None. (calib 은 옵셔널이므로 예외를 던지지 않는다)"""
    for line in original_calib:
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()
    return None


def parse_calib(original_calib):
    """
    Original_calib 리스트에서 P2/R0_rect/Tr_velo_to_cam 파싱.

    calib 은 옵셔널이다. 업로드에 calib/ 이 없으면 오토라벨링 API 가
    Original_calib: [] 로 내려주며, 이 경우 세 값 모두 None 을 반환한다.
    일부 라인만 없는 경우에도 해당 값만 None 이 되고 나머지는 정상 파싱한다.

    반환:
        camera_intrinsic: [[fx, 0, cx], [0, fy, cy], [0, 0, 1]]  (3×3 행렬) 또는 None
        translation:      [x, y, z]                                        또는 None
        rotation:         [w, x, y, z]                                     또는 None
    """
    original_calib = original_calib or []

    camera_intrinsic = translation = rotation = None

    # P2: 3x4 (KITTI 전방 컬러 카메라) → 앞 3x3 = 내부 파라미터 행렬
    p2_line = _find_calib_line(original_calib, "P2")
    if p2_line:
        P2 = np.array([float(x) for x in p2_line.split()]).reshape(3, 4)
        camera_intrinsic = P2[:, :3].tolist()

    # Tr_velo_to_cam: 3x4 → 마지막 열 = 평행이동
    tr_line = _find_calib_line(original_calib, "Tr_velo_to_cam")
    if tr_line:
        Tr = np.array([float(x) for x in tr_line.split()]).reshape(3, 4)
        translation = Tr[:, 3].tolist()

    # R0_rect: 3x3 → 쿼터니언 [w, x, y, z] (scipy 는 [x,y,z,w] 순으로 반환)
    r_line = _find_calib_line(original_calib, "R0_rect")
    if r_line:
        R = np.array([float(x) for x in r_line.split()]).reshape(3, 3)
        q = Rotation.from_matrix(R).as_quat()
        rotation = [q[3], q[0], q[1], q[2]]

    return camera_intrinsic, translation, rotation


def _json_or_none(value):
    """
    JSON 컬럼용 직렬화. value 가 None 이면 SQL NULL 로 넣기 위해 None 을 그대로 반환한다.

    json.dumps(None) 은 문자열 'null' 을 만들고 MySQL JSON 컬럼은 이를 SQL NULL 이
    아니라 "JSON null 값"으로 저장하기 때문에 IS NULL 조회에서 누락된다.
    adds.calibrated_sensor 의 기존 행들은 모두 SQL NULL 관례를 따르므로 여기에 맞춘다.
    """
    return None if value is None else json.dumps(value)


def resolve_category_token(class_name, category_dict, cur):
    """
    bbox class 이름을 adds.category.token 으로 해석.

    1) 완전 일치 (대소문자 무시)
    2) suffix 매칭 (".class" 로 끝나는 name 탐색, 예: Car → vehicle.car)
    3) fallback — adds.category 에 신규 삽입

    매칭된 경우는 adds 의 기존 token 을 그대로 재사용하므로 삽입하지 않는다.
    """
    lower = class_name.lower()

    for name, token in category_dict.items():
        if name.lower() == lower:
            print(f"[완전일치] '{class_name}' → '{name}'")
            return token

    suffix = f".{lower}"
    matches = [(name, token) for name, token in category_dict.items()
               if name.lower().endswith(suffix)]
    if len(matches) == 1:
        matched_name, matched_token = matches[0]
        print(f"[suffix매칭] '{class_name}' → '{matched_name}'")
        return matched_token
    if len(matches) > 1:
        print(f"[경고] category '{class_name}' 다중 매칭: "
              f"{[m[0] for m in matches]}, 첫 번째 사용")
        return matches[0][1]

    # fallback — 공유 마스터이므로 adds 에 직접 삽입한다.
    # staging 에만 넣으면 copy_to_adds 전까지 adds 에서 조회되지 않아
    # 다음 실행에서 같은 class 에 또 새 토큰이 발급된다(중복 누적).
    new_token = stable_token('category', class_name)
    cur.execute(f"USE {ADDS_DB}")
    cur.execute(
        "INSERT IGNORE INTO category (token, name) VALUES (%s, %s)",
        (new_token, class_name),
    )
    category_dict[class_name] = new_token
    print(f"[신규] adds.category 삽입: '{class_name}' → {new_token}")
    return new_token


# --- job_status 콜백 ------------------------------------------------------

def _update_job_status(context, status: str) -> None:
    """DAG 성공/실패 콜백에서 adds.job_status 를 업데이트한다."""
    conf = (context['dag_run'].conf or {})
    job_id = conf.get('job_id')
    if not job_id:
        print("[job_status] job_id 없음, 상태 업데이트 스킵")
        return
    try:
        mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
        conn = mysql_hook.get_conn()
        cur = conn.cursor()
        cur.execute(f"USE {ADDS_DB}")
        cur.execute(
            "UPDATE job_status SET status = %s, updated_at = %s WHERE job_id = %s",
            (status, datetime.now(KST), job_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[job_status] {status} | job_id={job_id}")
    except Exception as exc:
        print(f"[job_status] UPDATE failed | job_id={job_id} status={status} error={exc}")


def on_dag_success(context) -> None:
    _update_job_status(context, "COMPLETE")


def on_dag_failure(context) -> None:
    _update_job_status(context, "FAIL")


# --- Task 구현 ------------------------------------------------------------

def dataset_log_sensor_insert(**kwargs):
    """
    - dataset ("KITTI Auto-Labeled") : 공유 마스터 → adds 직접 (있으면 재사용)
    - sensor  (AUTO_LABEL_CAM / cam) : 공유 마스터 → adds 직접 (있으면 재사용)
    - log     (업로드당 1건)          : job 단위   → staging

    공유 마스터를 staging 이 아니라 adds 에 직접 넣는 이유:
    staging 에만 넣으면 copy_to_adds 가 돌기 전까지 adds 에서 조회되지 않아
    실행마다 새 토큰이 발급된다(dataset 6건 중복의 원인이었다).

    각 token 을 XCom push.
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"USE {ADDS_DB}")

    # dataset: adds 에 있으면 그 token 사용, 없으면 adds 에 삽입.
    # adds.dataset 에는 UNIQUE 제약이 없어 SELECT 결과에만 의존한다.
    # (동시 실행 시 중복 가능 — UNIQUE(name) 추가는 별도 작업)
    cur.execute("SELECT token FROM dataset WHERE name = %s", (DATASET_NAME,))
    row = cur.fetchone()
    if row:
        dataset_token = row[0]
        print(f"[adds 재사용] dataset_token = {dataset_token}")
    else:
        dataset_token = stable_token('dataset', DATASET_NAME)
        cur.execute(
            "INSERT INTO dataset (token, name) VALUES (%s, %s)",
            (dataset_token, DATASET_NAME),
        )
        print(f"[adds 신규] dataset_token = {dataset_token}")

    # sensor: 동일 패턴 (PK = token)
    cur.execute(
        "SELECT token FROM sensor WHERE channel = %s AND modality = %s",
        (SENSOR_CHANNEL, SENSOR_MODALITY),
    )
    row = cur.fetchone()
    if row:
        sensor_token = row[0]
        print(f"[adds 재사용] sensor_token = {sensor_token}")
    else:
        sensor_token = stable_token('sensor', SENSOR_CHANNEL, SENSOR_MODALITY)
        cur.execute(
            "INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s, %s, %s)",
            (sensor_token, SENSOR_CHANNEL, SENSOR_MODALITY),
        )
        print(f"[adds 신규] sensor_token = {sensor_token}")

    # log: 업로드(job)당 1건. 서로 무관한 업로드를 하나의 주행 기록으로 묶지 않는다.
    # device/date_captured/location 은 알 수 없어 NULL.
    # logfile 은 원본 수집 로그 파일명을 담는 컬럼이므로 여기서는 쓰지 않는다.
    log_token = stable_token('log', job_id)
    cur.execute(f"USE {STAGING_DB}")
    cur.execute(
        "INSERT IGNORE INTO log "
        "(token, dataset_token, device, date_captured, location, "
        " driving_distance, device_wlh, logfile) "
        "VALUES (%s, %s, NULL, NULL, NULL, NULL, NULL, NULL)",
        (log_token, dataset_token),
    )
    print(f"[업로드당 1건] log_token = {log_token}")

    conn.commit()
    cur.close()
    conn.close()

    ti.xcom_push(key='dataset_token', value=dataset_token)
    ti.xcom_push(key='log_token', value=log_token)
    ti.xcom_push(key='sensor_token', value=sensor_token)


def category_annotation_type_insert(**kwargs):
    """
    - adds.category 전체 조회하여 name→token 딕셔너리 XCom push (정규화 매칭용).
      신규 class 는 resolve_category_token() 이 adds 에 직접 삽입한다.
    - adds.annotation_type 에 "bbox_image2d" 없으면 adds 에 직접 삽입.
    """
    ti = kwargs['ti']
    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # category: adds 에서 전체 조회 (정규화 매칭용)
    cur.execute(f"USE {ADDS_DB}")
    cur.execute("SELECT name, token FROM category")
    category_dict = {name: token for name, token in cur.fetchall()}
    print(f"기존 category 수 (adds): {len(category_dict)}")

    # annotation_type: PK 가 name 이므로 INSERT IGNORE 로 충분하다.
    # 이미 있으면 adds 의 기존 description 이 그대로 유지된다.
    cur.execute(
        "INSERT IGNORE INTO annotation_type (name, description) VALUES (%s, %s)",
        (ANNOTATION_TYPE_NAME, ANNOTATION_TYPE_DESC),
    )
    print(f"[adds] annotation_type '{ANNOTATION_TYPE_NAME}' 확인/삽입")

    conn.commit()
    cur.close()
    conn.close()

    ti.xcom_push(key='category_dict', value=category_dict)


def scene_insert(**kwargs):
    """
    Job UUID = scene 1건. first/last_frame_token 은 이후 UPDATE 로 채움.
    auto_label.scene 에 INSERT IGNORE.

    base_path 우선순위:
      1) dag_run.conf['base_path']  — FastAPI 트리거 시 전달 (예: "v0.1/test_kitti/image_2/")
      2) fallback: "output/{job_id}/"
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)

    log_token = ti.xcom_pull(task_ids='dataset_log_sensor_insert', key='log_token')
    if not log_token:
        raise ValueError("log_token 을 XCom 으로 받지 못했습니다.")

    conf = kwargs['dag_run'].conf or {}
    base_path = conf.get('base_path') or f"output/{job_id}/"

    scene_token = uuid.UUID(job_id).hex
    json_files = list_json_files(job_id)
    num_frames = len(json_files)

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"USE {STAGING_DB}")
    cur.execute(
        "INSERT IGNORE INTO scene "
        "(token, log_token, base_path, num_frames, "
        " first_frame_token, last_frame_token, goal_ego_pose_token) "
        "VALUES (%s, %s, %s, %s, NULL, NULL, NULL)",
        (scene_token, log_token, base_path, num_frames),
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"scene_token = {scene_token} / num_frames = {num_frames}")


def calibrated_sensor_insert(**kwargs):
    """
    각 JSON 의 Original_calib 에서 P2/R0_rect/Tr_velo_to_cam 파싱.
    JSON 파일 인덱스 순서와 동일한 calib_tokens 리스트를 XCom push.

    calib 이 없는 업로드에서도 행은 프레임마다 그대로 생성한다.
    (calib 값 3개만 NULL) frame_data.calibrated_sensor_token → sensor 연결을
    유지하고, 하류 태스크의 길이 검사·행 수 카운트를 그대로 쓸 수 있기 때문이다.
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    sensor_token = ti.xcom_pull(task_ids='dataset_log_sensor_insert', key='sensor_token')
    if not sensor_token:
        raise ValueError("sensor_token 을 XCom 으로 받지 못했습니다.")

    json_files = list_json_files(job_id)
    calib_tokens = []
    rows = []
    no_calib_count = 0

    for i, json_file in enumerate(json_files):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # calib 은 옵셔널 — 키 자체가 없거나 [] 이면 세 값 모두 None 이 된다.
        camera_intrinsic, translation, rotation = parse_calib(data.get("Original_calib"))
        if camera_intrinsic is None and translation is None and rotation is None:
            no_calib_count += 1

        token = stable_token('calibrated_sensor', job_id, i)
        calib_tokens.append(token)
        rows.append((
            token,
            sensor_token,
            _json_or_none(camera_intrinsic),
            _json_or_none(translation),
            _json_or_none(rotation),
        ))

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"USE {STAGING_DB}")
    cur.executemany(
        "INSERT IGNORE INTO calibrated_sensor "
        "(token, sensor_token, camera_intrinsic, translation, rotation, "
        " height_from_ground, distortion) "
        "VALUES (%s, %s, %s, %s, %s, NULL, NULL)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"calibrated_sensor {len(rows)} rows 삽입 "
          f"(calib 없음 {no_calib_count} rows — 보정값 3개 NULL)")
    ti.xcom_push(key='calib_tokens', value=calib_tokens)


def frame_insert(**kwargs):
    """
    패턴 C — frame_tokens 전체 선생성 후 linked list (prev/next) 로 배치 삽입.
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    scene_token = uuid.UUID(job_id).hex

    json_files = list_json_files(job_id)
    n = len(json_files)
    frame_tokens = [stable_token('frame', job_id, i) for i in range(n)]

    rows = []
    for i in range(n):
        prev_token = frame_tokens[i - 1] if i > 0 else None
        next_token = frame_tokens[i + 1] if i < n - 1 else None
        rows.append((frame_tokens[i], scene_token, prev_token, next_token))

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"USE {STAGING_DB}")
    cur.executemany(
        "INSERT IGNORE INTO frame "
        "(token, timestamp, scene_token, prev, next) "
        "VALUES (%s, NULL, %s, %s, %s)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"frame {n} rows 삽입")
    ti.xcom_push(key='frame_tokens', value=frame_tokens)


def frame_data_insert(**kwargs):
    """
    frame_data_tokens 선생성 + frame_tokens/calib_tokens 인덱스 대응.
    Image_information (height/width/file_name/format) 사용.
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)

    calib_tokens = ti.xcom_pull(task_ids='calibrated_sensor_insert', key='calib_tokens')
    frame_tokens = ti.xcom_pull(task_ids='frame_insert', key='frame_tokens')
    if not calib_tokens or not frame_tokens:
        raise ValueError("calib_tokens / frame_tokens XCom 수신 실패")

    json_files = list_json_files(job_id)
    n = len(json_files)
    if not (len(calib_tokens) == len(frame_tokens) == n):
        raise ValueError(
            f"token 길이 불일치: json={n}, calib={len(calib_tokens)}, frame={len(frame_tokens)}"
        )

    frame_data_tokens = [stable_token('frame_data', job_id, i) for i in range(n)]
    rows = []

    for i, json_file in enumerate(json_files):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        image_info = data["Auto_labeling"]["Image_information"]
        height = image_info["height"]
        width = image_info["width"]
        file_name = image_info["file_name"]
        file_format = image_info["format"]

        prev_token = frame_data_tokens[i - 1] if i > 0 else None
        next_token = frame_data_tokens[i + 1] if i < n - 1 else None

        rows.append((
            frame_data_tokens[i],
            frame_tokens[i],
            calib_tokens[i],
            height,
            width,
            file_name,
            file_format,
            prev_token,
            next_token,
        ))

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"USE {STAGING_DB}")
    cur.executemany(
        "INSERT IGNORE INTO frame_data "
        "(token, frame_token, ego_pose_token, calibrated_sensor_token, "
        " timestamp, is_key_frame, height, width, file_name, file_format, prev, next) "
        "VALUES (%s, %s, NULL, %s, NULL, 1, %s, %s, %s, %s, %s, %s)",
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"frame_data {n} rows 삽입")
    ti.xcom_push(key='frame_data_tokens', value=frame_data_tokens)


def scenario_scenarios_in_scene_insert(**kwargs):
    """
    Time / Weather 조합을 job 내 distinct 단위로 scenario 1건 + scenarios_in_scene 1건 생성.
    둘 다 staging 에 삽입.

    프레임마다 만들지 않는 이유: scenarios_in_scene 은 scene↔scenario 만 연결하고
    프레임과 연결하는 컬럼이 없다. 따라서 같은 scene 아래 description 이 동일한
    행들은 애초에 구별이 불가능하므로, 1건으로 합쳐도 조회 가능한 정보는 같다.

    중복 제거 범위는 job 내부다. job_id 를 토큰에 포함시키므로 adds.scenario
    전역을 조회할 필요 없이 job 간에는 자동으로 분리된다.
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    scene_token = uuid.UUID(job_id).hex
    json_files = list_json_files(job_id)

    seen = {}            # description -> scenario_token
    scenario_rows = []   # (token, description)
    sis_rows = []        # (scene_token, scenario_token)

    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        time_class = data["Auto_labeling"]["Time_information"]["class"]
        weather_class = data["Auto_labeling"]["Weather_information"]["class"]
        description = f"{time_class},{weather_class}"

        if description in seen:
            continue

        scenario_token = stable_token('scenario', job_id, description)
        seen[description] = scenario_token
        scenario_rows.append((scenario_token, description))
        sis_rows.append((scene_token, scenario_token))

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # scenario + scenarios_in_scene: staging
    cur.execute(f"USE {STAGING_DB}")
    cur.executemany(
        "INSERT IGNORE INTO scenario (token, description) VALUES (%s, %s)",
        scenario_rows,
    )
    cur.executemany(
        "INSERT IGNORE INTO scenarios_in_scene (scene_token, scenario_token) "
        "VALUES (%s, %s)",
        sis_rows,
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"scenario {len(scenario_rows)} / scenarios_in_scene {len(sis_rows)} rows 삽입 "
          f"(프레임 {len(json_files)}개 → distinct {len(scenario_rows)}개)")
    ti.xcom_push(key='scenario_added_rows', value=len(scenario_rows))


def instance_frame_annotation_new_insert(**kwargs):
    """
    bbox 1건 = instance 1건 (tracking 정보 없음).
    instance.token / frame_annotation_new.token 을 미리 생성(패턴 E).
    first/last_frame_annotation_token = fa_token, num_annotations = 1.
    마지막에 scene.first/last_frame_token 을 UPDATE (패턴 D).
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    scene_token = uuid.UUID(job_id).hex

    frame_tokens = ti.xcom_pull(task_ids='frame_insert', key='frame_tokens')
    frame_data_tokens = ti.xcom_pull(task_ids='frame_data_insert', key='frame_data_tokens')
    category_dict = ti.xcom_pull(
        task_ids='category_annotation_type_insert', key='category_dict',
    ) or {}

    if not frame_tokens or not frame_data_tokens:
        raise ValueError("frame_tokens / frame_data_tokens XCom 수신 실패")

    json_files = list_json_files(job_id)
    n = len(json_files)
    if not (len(frame_tokens) == len(frame_data_tokens) == n):
        raise ValueError(
            f"token 길이 불일치: json={n}, frame={len(frame_tokens)}, "
            f"frame_data={len(frame_data_tokens)}"
        )

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    # resolve_category_token() 은 신규 class 발견 시 adds.category 에 직접 INSERT 한다.
    initial_category_count = len(category_dict)
    instance_rows = []
    fa_rows = []

    for i, json_file in enumerate(json_files):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        bbox_list = (
            data["Auto_labeling"]["Detection_information"].get("bbox_info") or []
        )

        for j, bbox in enumerate(bbox_list):
            class_name = bbox["class"]
            category_token = resolve_category_token(class_name, category_dict, cur)

            inst_token = stable_token('instance', job_id, i, j)
            fa_token = stable_token('frame_annotation', job_id, i, j)

            instance_rows.append((
                inst_token,
                category_token,
                fa_token,   # first
                fa_token,   # last
                1,          # num_annotations
                None,       # size
            ))

            bbox_2d = [
                bbox["bbox_x1"],
                bbox["bbox_y1"],
                bbox["bbox_x2"],
                bbox["bbox_y2"],
            ]

            fa_rows.append((
                fa_token,
                frame_tokens[i],
                frame_data_tokens[i],
                inst_token,
                ANNOTATION_TYPE_NAME,
                json.dumps(bbox_2d),
            ))

    # resolve_category_token() 이 USE adds 로 전환했을 수 있으므로 staging 으로 복귀
    cur.execute(f"USE {STAGING_DB}")

    if instance_rows:
        cur.executemany(
            "INSERT IGNORE INTO instance "
            "(token, category_token, first_frame_annotation_token, "
            " last_frame_annotation_token, num_annotations, size) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            instance_rows,
        )

    if fa_rows:
        cur.executemany(
            "INSERT IGNORE INTO frame_annotation_new "
            "(token, frame_token, frame_data_token, instance_token, "
            " visibility_token, annotation_type_name, is_lidar_synced, "
            " size_wlh, translation_center, rotation_orientation, "
            " bbox_image3d, bbox_image2d, num_lidar_pts, num_radar_pts, "
            " attribute_tokens, frame_aligned, description, prev, next, velocity) "
            "VALUES (%s, %s, %s, %s, NULL, %s, NULL, "
            "        NULL, NULL, NULL, NULL, %s, NULL, NULL, "
            "        NULL, NULL, NULL, NULL, NULL, NULL)",
            fa_rows,
        )

    # 패턴 D — scene.first/last_frame_token 확정
    first_frame_token = frame_tokens[0]
    last_frame_token = frame_tokens[-1]
    cur.execute(
        "UPDATE scene SET first_frame_token = %s, last_frame_token = %s "
        "WHERE token = %s",
        (first_frame_token, last_frame_token, scene_token),
    )

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"instance {len(instance_rows)} / frame_annotation_new {len(fa_rows)} rows 삽입, "
        f"scene({scene_token}) first/last_frame_token 업데이트"
    )
    ti.xcom_push(key='instance_count', value=len(instance_rows))
    ti.xcom_push(key='frame_annotation_count', value=len(fa_rows))
    ti.xcom_push(key='new_category_count', value=len(category_dict) - initial_category_count)


def copy_to_adds(**kwargs):
    """
    스테이징 DB(auto_label) 에서 **이번 job 의 데이터만** adds 로 복사.

    공유 마스터(dataset/sensor/category/annotation_type)는 각 태스크가 adds 에
    직접 삽입하므로 복사 대상이 아니다.

    job 필터가 필수인 이유: DAG 는 staging 을 비우지 않으므로 테이블 전체를
    복사하면 실행마다 이전 job 행까지 재복사된다. adds.frame_annotation_new 는
    PK 가 없어 INSERT IGNORE 로도 걸러지지 않는다.

    재복사 방지: adds.scene 에 이번 scene_token 이 이미 있으면 전체를 스킵한다.
    (커밋은 마지막에 1회만 하므로 "일부만 복사된" 중간 상태가 남지 않는다.)
    """
    job_id = get_job_id(kwargs)
    scene_token = uuid.UUID(job_id).hex

    # 이번 job 의 frame / frame_data token 집합 (staging 기준)
    frame_subq = f"SELECT token FROM {STAGING_DB}.frame WHERE scene_token = %(scene)s"

    # (테이블, WHERE 절) — FK 의존성 순서를 지킨다
    steps = [
        ('log',
         f"token = (SELECT log_token FROM {STAGING_DB}.scene WHERE token = %(scene)s)"),
        ('scene',
         "token = %(scene)s"),
        ('calibrated_sensor',
         f"token IN (SELECT calibrated_sensor_token FROM {STAGING_DB}.frame_data "
         f"          WHERE frame_token IN ({frame_subq}))"),
        ('frame',
         "scene_token = %(scene)s"),
        ('frame_data',
         f"frame_token IN ({frame_subq})"),
        ('scenario',
         f"token IN (SELECT scenario_token FROM {STAGING_DB}.scenarios_in_scene "
         f"          WHERE scene_token = %(scene)s)"),
        ('scenarios_in_scene',
         "scene_token = %(scene)s"),
        ('instance',
         f"token IN (SELECT instance_token FROM {STAGING_DB}.frame_annotation_new "
         f"          WHERE frame_token IN ({frame_subq}))"),
        ('frame_annotation_new',
         f"frame_token IN ({frame_subq})"),
    ]

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM {ADDS_DB}.scene WHERE token = %s", (scene_token,))
    if cur.fetchone():
        print(f"[skip] 이미 adds 에 복사된 job 입니다 | scene_token={scene_token}")
        cur.close()
        conn.close()
        return

    total = 0
    for table, where in steps:
        sql = (
            f"INSERT IGNORE INTO {ADDS_DB}.{table} "
            f"SELECT * FROM {STAGING_DB}.{table} WHERE {where}"
        )
        cur.execute(sql, {'scene': scene_token})
        print(f"[copy] {table}: {cur.rowcount} rows")
        total += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"copy_to_adds 완료 | scene_token={scene_token} | 총 {total} rows")


def provenance_insert(**kwargs):
    """
    파이프라인 최종 단계 — 이력 관리 3개 테이블 일괄 INSERT.
      1) dataset_versions : 신규 버전 발급
      2) dataset_changes  : 테이블별 추가 행 기록 (XCom에서 카운트 수집)
      3) dataset_provenance : 출처 및 처리 경로 기록
    """
    ti = kwargs['ti']
    job_id = get_job_id(kwargs)
    conf = kwargs['dag_run'].conf or {}

    # --- XCom pull --------------------------------------------------------
    dataset_token = ti.xcom_pull(
        task_ids='dataset_log_sensor_insert', key='dataset_token',
    )
    calib_tokens = ti.xcom_pull(
        task_ids='calibrated_sensor_insert', key='calib_tokens',
    ) or []
    frame_tokens = ti.xcom_pull(
        task_ids='frame_insert', key='frame_tokens',
    ) or []
    frame_data_tokens = ti.xcom_pull(
        task_ids='frame_data_insert', key='frame_data_tokens',
    ) or []
    scenario_added_rows = ti.xcom_pull(
        task_ids='scenario_scenarios_in_scene_insert', key='scenario_added_rows',
    ) or 0
    instance_count = ti.xcom_pull(
        task_ids='instance_frame_annotation_new_insert', key='instance_count',
    ) or 0
    frame_annotation_count = ti.xcom_pull(
        task_ids='instance_frame_annotation_new_insert', key='frame_annotation_count',
    ) or 0
    new_category_count = ti.xcom_pull(
        task_ids='instance_frame_annotation_new_insert', key='new_category_count',
    ) or 0

    # dataset_changes 대상 테이블 및 추가 행 수 (0이면 INSERT 스킵)
    table_counts = [
        ('log',                  1),
        ('scene',                1),
        ('calibrated_sensor',    len(calib_tokens)),
        ('frame',                len(frame_tokens)),
        ('frame_data',           len(frame_data_tokens)),
        ('scenario',             scenario_added_rows),
        ('scenarios_in_scene',   scenario_added_rows),
        ('instance',             instance_count),
        ('frame_annotation_new', frame_annotation_count),
        ('category',             new_category_count),
    ]

    # source: base_path 에서 zip 파일명 파싱
    base_path = conf.get('base_path', '')
    parts = base_path.strip('/').split('/')
    source = parts[1] + '.zip' if len(parts) > 1 else base_path

    executed_by = conf.get('user_id') or 'unknown'

    now = datetime.now(KST)

    mysql_hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute(f"USE {PROVENANCE_DB}")

    # --- 1) dataset_versions ----------------------------------------------
    # 직전 버전 조회
    cur.execute(
        "SELECT version_id, version_name FROM dataset_versions "
        "ORDER BY created_at DESC LIMIT 1"
    )
    prev_row = cur.fetchone()
    if not prev_row:
        raise ValueError(
            "dataset_versions 에 기준 버전이 없습니다. "
            "clear_auto_label_staging.sql 실행 후 재시도하세요."
        )
    from_version_id, prev_name = prev_row
    # v{major}.{minor} 파싱 후 minor +1
    try:
        major, minor = prev_name.lstrip('v').split('.')
        version_name = f"v{major}.{int(minor) + 1}"
    except Exception:
        version_name = f"{prev_name}_next"

    new_version_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO dataset_versions (version_id, version_name, created_by, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        (new_version_id, version_name, executed_by, now, now),
    )
    print(f"[dataset_versions] {version_name} (id={new_version_id})")

    # --- 2) dataset_changes -----------------------------------------------
    changes_inserted = 0
    for table_name, added_rows in table_counts:
        if added_rows == 0:
            print(f"[dataset_changes] {table_name}: 0 rows, 스킵")
            continue
        change_id = str(uuid.uuid4())
        change_log = str(added_rows)
        cur.execute(
            "INSERT INTO dataset_changes "
            "(change_id, from_version_id, to_version_id, change_category, "
            " change_action, target, change_log, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (change_id, from_version_id, new_version_id,
             'row', 'add', table_name, change_log, now, now),
        )
        print(f"[dataset_changes] {table_name}: {change_log}")
        changes_inserted += 1

    # --- 3) dataset_provenance --------------------------------------------
    provenance_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO dataset_provenance "
        "(provenance_id, version_id, source, processing_step, "
        " ingestion_mode, executed_by, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (provenance_id, new_version_id, source,
         'auto_labeling', 'system', executed_by, now, now),
    )
    print(f"[dataset_provenance] source={source}, version={version_name}")

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"provenance_insert 완료 | version={version_name} | "
        f"changes={changes_inserted}건 | job_id={job_id}"
    )


# --- DAG ------------------------------------------------------------------

with DAG(
    'auto_labeling_insert',
    default_args=default_args,
    description='KITTI auto-labeling API 결과를 auto_label → adds 로 적재',
    schedule_interval=None,
    params={
        "job_id": Param("", type="string", description="Job ID를 입력하세요"),
        "base_path": Param("", type="string", description="S3 이미지 경로 prefix (예: v0.1/test_kitti/image_2/)"),
    },
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    catchup=False,
    tags=['auto_labeling', 'adds'],
) as dag:

    dataset_log_sensor_insert_task = PythonOperator(
        task_id='dataset_log_sensor_insert',
        python_callable=dataset_log_sensor_insert,
    )

    category_annotation_type_insert_task = PythonOperator(
        task_id='category_annotation_type_insert',
        python_callable=category_annotation_type_insert,
    )

    scene_insert_task = PythonOperator(
        task_id='scene_insert',
        python_callable=scene_insert,
    )

    calibrated_sensor_insert_task = PythonOperator(
        task_id='calibrated_sensor_insert',
        python_callable=calibrated_sensor_insert,
    )

    frame_insert_task = PythonOperator(
        task_id='frame_insert',
        python_callable=frame_insert,
    )

    frame_data_insert_task = PythonOperator(
        task_id='frame_data_insert',
        python_callable=frame_data_insert,
    )

    scenario_scenarios_in_scene_insert_task = PythonOperator(
        task_id='scenario_scenarios_in_scene_insert',
        python_callable=scenario_scenarios_in_scene_insert,
    )

    instance_frame_annotation_new_insert_task = PythonOperator(
        task_id='instance_frame_annotation_new_insert',
        python_callable=instance_frame_annotation_new_insert,
    )

    copy_to_adds_task = PythonOperator(
        task_id='copy_to_adds',
        python_callable=copy_to_adds,
    )

    provenance_insert_task = PythonOperator(
        task_id='provenance_insert',
        python_callable=provenance_insert,
    )

    # 의존성
    [dataset_log_sensor_insert_task, category_annotation_type_insert_task] >> scene_insert_task
    scene_insert_task >> [calibrated_sensor_insert_task, frame_insert_task] >> frame_data_insert_task
    frame_data_insert_task >> [
        scenario_scenarios_in_scene_insert_task,
        instance_frame_annotation_new_insert_task,
    ]
    category_annotation_type_insert_task >> instance_frame_annotation_new_insert_task
    [
        scenario_scenarios_in_scene_insert_task,
        instance_frame_annotation_new_insert_task,
    ] >> copy_to_adds_task
    copy_to_adds_task >> provenance_insert_task
