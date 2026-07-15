"""
lyft_level5_insert.py

Lyft Level 5 데이터셋을 staging DB(lyft_level5)에 적재하는 Airflow DAG.
검증 완료 후 copy_to_adds 태스크의 주석을 해제해 adds DB에 복사한다.

설계 문서: airflow/docs/lyft_level5_etl_mapping.md
참고 DAG:  bdd100k_insert.py, nuScenes_insert.py

원천 데이터: /home/ubuntu/airflow/dataset/filtered/
Lyft token: 64자 hex → MD5 해시 → 32자 (to_token())
"""

import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone

import ijson

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook


KST = timezone(timedelta(hours=9))

# ── 상수 ──────────────────────────────────────────────────────────────────────
SOURCE_DIR    = "/home/ubuntu/airflow/dataset/filtered"
STAGING_DB    = "lyft_level5"
ADDS_DB       = "adds"
MYSQL_CONN_ID = "aws_mysql"

DATASET_NAME         = "lyft_level5"
ANNOTATION_TYPE_NAME = "lyft_annotation"
BATCH_SIZE           = 50_000

# 테스트 모드: 공유 마스터(category/attribute/scenario)도 staging DB에 씀.
# 검증 완료 후 MASTER_WRITE_DB = ADDS_DB 로 변경.
MASTER_WRITE_DB = STAGING_DB

# ── Lyft flat category → ADDS 계층 경로 ──────────────────────────────────────
CATEGORY_MAP = {
    "car":           "vehicle.car",
    "truck":         "vehicle.truck",
    "bus":           "vehicle.bus",
    "motorcycle":    "vehicle.motorcycle",
    "bicycle":       "vehicle.bicycle",
    "pedestrian":    "human.pedestrian",
    "other_vehicle": "vehicle.other_vehicle",
}


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────

def to_token(lyft_token: str) -> str | None:
    """Lyft 64자 hex token → MD5 해시 32자 token 변환."""
    if not lyft_token:
        return None
    return hashlib.md5(lyft_token.encode()).hexdigest()


def _get_conn():
    hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def _src(filename: str) -> str:
    return f"{SOURCE_DIR}/{filename}"


# ── DAG 정의 ──────────────────────────────────────────────────────────────────

default_args = {
    "owner": "eun",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lyft_level5_insert",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule_interval=None,
    catchup=False,
    tags=["lyft", "etl"],
) as dag:

    # ── Task 1: annotation_type_insert ────────────────────────────────────────
    @task
    def annotation_type_insert():
        """
        'lyft_annotation' 타입을 staging DB annotation_type 테이블에 등록한다.
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            f"INSERT IGNORE INTO {STAGING_DB}.annotation_type (name, description) "
            "VALUES (%s, %s)",
            (ANNOTATION_TYPE_NAME, "Lyft Level 5 3D bounding box annotation"),
        )
        conn.commit()
        print(f"[annotation_type] '{ANNOTATION_TYPE_NAME}' 등록 완료")

        cur.close()
        conn.close()

    # ── Task 2: category_insert ───────────────────────────────────────────────
    @task
    def category_insert():
        """
        category.json 7건을 CATEGORY_MAP으로 계층 경로 변환 후 처리한다.
        - adds.category에서 기존 {name: token} 맵 조회
        - 없는 항목만 MASTER_WRITE_DB.category에 INSERT
        - {md5(lyft_token): final_db_token} 맵 반환 → instance_insert에서 사용
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(f"SELECT name, token FROM {ADDS_DB}.category")
        existing = {name: token for name, token in cur.fetchall()}
        print(f"[category] 기존 {ADDS_DB}.category: {len(existing)}건")

        with open(_src("category.json"), encoding="utf-8") as f:
            categories = json.load(f)

        lyft_md5_to_db_token = {}  # {md5(lyft_64char_token): final_db_token}
        new_rows = []

        for cat in categories:
            lyft_token = cat["token"]
            lyft_name  = cat["name"]
            adds_name  = CATEGORY_MAP.get(lyft_name, lyft_name)

            if adds_name in existing:
                db_token = existing[adds_name]
            else:
                db_token = to_token(lyft_token)
                existing[adds_name] = db_token
                new_rows.append((db_token, adds_name, None, None, None))

            lyft_md5_to_db_token[to_token(lyft_token)] = db_token

        if new_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {MASTER_WRITE_DB}.category "
                "(token, name, description, default_scale, `index`) "
                "VALUES (%s, %s, %s, %s, %s)",
                new_rows,
            )
            conn.commit()
            print(f"[category] 신규 {len(new_rows)}건 → {MASTER_WRITE_DB}.category")
        else:
            print("[category] 신규 없음 (모두 기존 token 재사용)")

        cur.close()
        conn.close()
        return lyft_md5_to_db_token

    # ── Task 3: attribute_insert ──────────────────────────────────────────────
    @task
    def attribute_insert():
        """
        attribute.json 15건을 Lyft 고유 포맷(object_action_*) 그대로 처리한다.
        - adds.attribute에서 기존 항목 조회
        - 없는 항목만 MASTER_WRITE_DB.attribute에 INSERT
        - {md5(lyft_token): final_db_token} 맵 반환 → frame_annotation_insert에서 사용
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(f"SELECT name, token FROM {ADDS_DB}.attribute")
        existing = {name: token for name, token in cur.fetchall()}
        print(f"[attribute] 기존 {ADDS_DB}.attribute: {len(existing)}건")

        with open(_src("attribute.json"), encoding="utf-8") as f:
            attributes = json.load(f)

        lyft_md5_to_db_token = {}
        new_rows = []

        for attr in attributes:
            lyft_token = attr["token"]
            name       = attr["name"]  # "object_action_*" Lyft 고유 포맷 그대로

            if name in existing:
                db_token = existing[name]
            else:
                db_token = to_token(lyft_token)
                existing[name] = db_token
                new_rows.append((db_token, name, None))

            lyft_md5_to_db_token[to_token(lyft_token)] = db_token

        if new_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {MASTER_WRITE_DB}.attribute "
                "(token, name, description) VALUES (%s, %s, %s)",
                new_rows,
            )
            conn.commit()
            print(f"[attribute] 신규 {len(new_rows)}건 → {MASTER_WRITE_DB}.attribute")
        else:
            print("[attribute] 신규 없음 (모두 기존 token 재사용)")

        cur.close()
        conn.close()
        return lyft_md5_to_db_token

    # ── Task 4: sensor_insert ─────────────────────────────────────────────────
    @task
    def sensor_insert():
        """
        sensor.json 10건 (카메라 7채널 + LiDAR 3채널)을 staging DB에 INSERT한다.
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("sensor.json"), encoding="utf-8") as f:
            sensors = json.load(f)

        rows = [
            (to_token(s["token"]), s["channel"], s["modality"])
            for s in sensors
        ]
        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.sensor (token, channel, modality) "
            "VALUES (%s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[sensor] {len(rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 5: calibrated_sensor_insert ──────────────────────────────────────
    @task
    def calibrated_sensor_insert():
        """
        calibrated_sensor.json 92건을 staging DB에 INSERT한다.
        - LiDAR의 경우 camera_intrinsic 빈 배열 [] → NULL
        - height_from_ground = translation[2]
        sensor_insert 완료 후 실행 (sensor_token FK 보장)
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("calibrated_sensor.json"), encoding="utf-8") as f:
            cal_sensors = json.load(f)

        rows = []
        for cs in cal_sensors:
            camera_intrinsic = cs.get("camera_intrinsic")
            rows.append((
                to_token(cs["token"]),
                to_token(cs["sensor_token"]),
                json.dumps(cs["translation"]),
                json.dumps(cs["rotation"]),
                json.dumps(camera_intrinsic) if camera_intrinsic else None,
                cs["translation"][2],
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.calibrated_sensor "
            "(token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[calibrated_sensor] {len(rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 6: log_insert ────────────────────────────────────────────────────
    @task
    def log_insert():
        """
        dataset 1건을 생성하고 log.json 20건을 staging DB에 INSERT한다.
        - logfile 빈 문자열 → NULL
        - driving_distance, device_wlh: Lyft에 없음 → NULL
        """
        conn = _get_conn()
        cur = conn.cursor()

        dataset_token = secrets.token_hex(16)
        cur.execute(
            f"INSERT IGNORE INTO {STAGING_DB}.dataset (token, name) VALUES (%s, %s)",
            (dataset_token, DATASET_NAME),
        )

        with open(_src("log.json"), encoding="utf-8") as f:
            logs = json.load(f)

        rows = []
        for log in logs:
            rows.append((
                to_token(log["token"]),
                dataset_token,
                log.get("vehicle") or None,
                log.get("date_captured") or None,
                log.get("location") or None,
                None,                              # driving_distance
                None,                              # device_wlh
                log.get("logfile") or None,
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.log "
            "(token, dataset_token, device, date_captured, location, "
            " driving_distance, device_wlh, logfile) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[log] dataset_token={dataset_token}, {len(rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 7: map_and_log_map_insert ────────────────────────────────────────
    @task
    def map_and_log_map_insert():
        """
        map.json 1건을 map 테이블에 INSERT하고,
        log_tokens 배열을 순회해 log_map 연결 레코드를 INSERT한다.
        - map.token: 이미 32자 → 변환 없이 사용
        - log_tokens: 64자 hex → to_token() 변환
        log_insert 완료 후 실행 (log_token FK 보장)
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("map.json"), encoding="utf-8") as f:
            maps = json.load(f)

        map_rows     = []
        log_map_rows = []

        for m in maps:
            map_token = m["token"]   # 이미 32자, 변환 불필요
            map_rows.append((map_token, m["category"], m["filename"]))
            for log_token_64 in m.get("log_tokens", []):
                log_map_rows.append((to_token(log_token_64), map_token))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.map (token, category, filename) "
            "VALUES (%s, %s, %s)",
            map_rows,
        )
        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.log_map (log_token, map_token) "
            "VALUES (%s, %s)",
            log_map_rows,
        )
        conn.commit()
        print(f"[map] {len(map_rows)}건, [log_map] {len(log_map_rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 8: scene_insert ──────────────────────────────────────────────────
    @task
    def scene_insert():
        """
        scene.json 20건을 staging DB에 INSERT한다.
        - first/last_sample_token → first/last_frame_token (이름 다름)
        - nbr_samples → num_frames (이름 다름)
        log_insert 완료 후 실행 (log_token FK 보장)
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("scene.json"), encoding="utf-8") as f:
            scenes = json.load(f)

        rows = []
        for s in scenes:
            rows.append((
                to_token(s["token"]),
                to_token(s["log_token"]),
                None,                                   # base_path
                s["nbr_samples"],                       # num_frames
                to_token(s["first_sample_token"]),      # first_frame_token
                to_token(s["last_sample_token"]),       # last_frame_token
                None,                                   # goal_ego_pose_token
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.scene "
            "(token, log_token, base_path, num_frames, "
            " first_frame_token, last_frame_token, goal_ego_pose_token) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[scene] {len(rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 9: ego_pose_insert ───────────────────────────────────────────────
    @task
    def ego_pose_insert():
        """
        ego_pose.json 7,657건을 ijson 스트리밍으로 읽어 staging DB에 INSERT한다.
        - pose_type="vehicle" 고정
        - coordinate_system, velocity, acceleration, angular_rate: Lyft에 없음 → NULL
        - 50,000건 배치 처리
        """
        conn = _get_conn()
        cur = conn.cursor()

        query = (
            f"INSERT IGNORE INTO {STAGING_DB}.ego_pose "
            "(token, timestamp, translation, rotation, pose_type) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        data = []

        with open(_src("ego_pose.json"), "rb") as f:
            for i, ep in enumerate(ijson.items(f, "item", use_float=True), 1):
                data.append((
                    to_token(ep["token"]),
                    str(ep["timestamp"]),
                    json.dumps(ep["translation"]),
                    json.dumps(ep["rotation"]),
                    "vehicle",
                ))
                if i % BATCH_SIZE == 0:
                    cur.executemany(query, data)
                    conn.commit()
                    print(f"[ego_pose] {i}건 처리 완료")
                    data = []

        if data:
            cur.executemany(query, data)
            conn.commit()
            print(f"[ego_pose] 마지막 {len(data)}건 완료")

        cur.close()
        conn.close()

    # ── Task 10: frame_insert ─────────────────────────────────────────────────
    @task
    def frame_insert():
        """
        sample.json 1,000건을 staging DB frame 테이블에 INSERT한다.
        - prev/next 빈 문자열 → NULL
        scene_insert 완료 후 실행 (scene_token FK 보장)
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("sample.json"), encoding="utf-8") as f:
            samples = json.load(f)

        rows = []
        for s in samples:
            rows.append((
                to_token(s["token"]),
                str(s["timestamp"]),
                to_token(s["scene_token"]),
                to_token(s["prev"]) if s.get("prev") else None,
                to_token(s["next"]) if s.get("next") else None,
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.frame "
            "(token, timestamp, scene_token, prev, next) "
            "VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[frame] {len(rows)}건 INSERT 완료")

        cur.close()
        conn.close()

    # ── Task 11: frame_data_insert ────────────────────────────────────────────
    @task
    def frame_data_insert():
        """
        sample_data.json 8,408건을 ijson 스트리밍으로 읽어 staging DB에 INSERT한다.
        - sample_token → frame_token (이름 다름)
        - filename → file_name, fileformat → file_format (이름 다름)
        - "jpeg" → "jpg" (file_format char(3) 컬럼 대응)
        - prev/next 빈 문자열 → NULL
        - 50,000건 배치 처리
        frame_insert, calibrated_sensor_insert, ego_pose_insert 완료 후 실행
        """
        conn = _get_conn()
        cur = conn.cursor()

        query = (
            f"INSERT IGNORE INTO {STAGING_DB}.frame_data "
            "(token, frame_token, ego_pose_token, calibrated_sensor_token, "
            " timestamp, is_key_frame, height, width, file_name, file_format, "
            " prev, next) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = []

        with open(_src("sample_data.json"), "rb") as f:
            for i, sd in enumerate(ijson.items(f, "item", use_float=True), 1):
                file_format = sd.get("fileformat", "")
                if file_format == "jpeg":
                    file_format = "jpg"   # char(3) 컬럼 대응

                data.append((
                    to_token(sd["token"]),
                    to_token(sd["sample_token"]),
                    to_token(sd["ego_pose_token"]),
                    to_token(sd["calibrated_sensor_token"]),
                    str(sd["timestamp"]),
                    int(sd.get("is_key_frame", True)),
                    sd.get("height"),
                    sd.get("width"),
                    sd.get("filename"),
                    file_format or None,
                    to_token(sd["prev"]) if sd.get("prev") else None,
                    to_token(sd["next"]) if sd.get("next") else None,
                ))

                if i % BATCH_SIZE == 0:
                    cur.executemany(query, data)
                    conn.commit()
                    print(f"[frame_data] {i}건 처리 완료")
                    data = []

        if data:
            cur.executemany(query, data)
            conn.commit()
            print(f"[frame_data] 마지막 {len(data)}건 완료")

        cur.close()
        conn.close()

    # ── Task 12: instance_insert ──────────────────────────────────────────────
    @task
    def instance_insert(category_token_map: dict):
        """
        instance.json 2,104건을 staging DB에 INSERT한다.
        - category_token: category_token_map {md5(lyft_token): db_token} 으로 조회
        - first/last_annotation_token → first/last_frame_annotation_token (이름 다름)
        - nbr_annotations → num_annotations (이름 다름)
        - size: Lyft에 없음 → NULL
        category_insert, frame_insert 완료 후 실행
        """
        conn = _get_conn()
        cur = conn.cursor()

        with open(_src("instance.json"), encoding="utf-8") as f:
            instances = json.load(f)

        rows = []
        skipped = 0
        for inst in instances:
            md5_cat = to_token(inst["category_token"])
            db_cat  = category_token_map.get(md5_cat)
            if db_cat is None:
                print(f"[경고] category_token 없음: {inst['category_token'][:16]}...")
                skipped += 1
                continue

            rows.append((
                to_token(inst["token"]),
                db_cat,
                to_token(inst["first_annotation_token"]),
                to_token(inst["last_annotation_token"]),
                inst["nbr_annotations"],
                None,   # size
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.instance "
            "(token, category_token, first_frame_annotation_token, "
            " last_frame_annotation_token, num_annotations, size) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        print(f"[instance] {len(rows)}건 INSERT 완료 (스킵: {skipped}건)")

        cur.close()
        conn.close()

    # ── Task 13: frame_annotation_insert ──────────────────────────────────────
    @task
    def frame_annotation_insert(attribute_token_map: dict):
        """
        sample_annotation.json 29,837건을 ijson 스트리밍으로 읽어
        staging DB frame_annotation_new 테이블에 INSERT한다.
        - sample_token → frame_token (이름 다름)
        - size → size_wlh, translation → translation_center, rotation → rotation_orientation
        - visibility_token "" → NULL (Lyft는 항상 빈 문자열)
        - num_lidar_pts = -1 → NULL (Lyft는 LiDAR 포인트 수 정보 없음)
        - attribute_tokens: 각 64자 token → md5 → attribute_token_map 조회
        - 50,000건 배치 처리
        instance_insert, frame_insert, annotation_type_insert 완료 후 실행
        """
        conn = _get_conn()
        cur = conn.cursor()

        query = (
            f"INSERT IGNORE INTO {STAGING_DB}.frame_annotation_new "
            "(token, frame_token, frame_data_token, instance_token, "
            " visibility_token, annotation_type_name, is_lidar_synced, "
            " size_wlh, translation_center, rotation_orientation, "
            " bbox_image3d, bbox_image2d, num_lidar_pts, num_radar_pts, "
            " attribute_tokens, frame_aligned, description, prev, next, velocity) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = []

        with open(_src("sample_annotation.json"), "rb") as f:
            for i, ann in enumerate(ijson.items(f, "item", use_float=True), 1):
                # visibility_token 빈 문자열 → NULL
                vis_token = ann.get("visibility_token") or None

                # num_lidar_pts = -1 → NULL
                num_lidar = ann.get("num_lidar_pts")
                if num_lidar is not None and num_lidar < 0:
                    num_lidar = None

                # attribute_tokens: 64자 → md5 → DB token 조회
                raw_attr = ann.get("attribute_tokens", [])
                db_attr  = [
                    attribute_token_map[to_token(t)]
                    for t in raw_attr
                    if to_token(t) in attribute_token_map
                ]
                attr_tokens_json = json.dumps(db_attr) if db_attr else None

                data.append((
                    to_token(ann["token"]),
                    to_token(ann["sample_token"]),          # frame_token
                    None,                                    # frame_data_token (없음)
                    to_token(ann["instance_token"]),
                    vis_token,
                    ANNOTATION_TYPE_NAME,
                    None,                                    # is_lidar_synced
                    json.dumps([float(x) for x in ann["size"]]),          # size_wlh
                    json.dumps([float(x) for x in ann["translation"]]),   # translation_center
                    json.dumps([float(x) for x in ann["rotation"]]),      # rotation_orientation
                    None,                                    # bbox_image3d
                    None,                                    # bbox_image2d
                    num_lidar,
                    ann.get("num_radar_pts"),
                    attr_tokens_json,
                    None,                                    # frame_aligned
                    None,                                    # description
                    to_token(ann["prev"]) if ann.get("prev") else None,
                    to_token(ann["next"]) if ann.get("next") else None,
                    None,                                    # velocity
                ))

                if i % BATCH_SIZE == 0:
                    cur.executemany(query, data)
                    conn.commit()
                    print(f"[frame_annotation] {i}건 처리 완료")
                    data = []

        if data:
            cur.executemany(query, data)
            conn.commit()
            print(f"[frame_annotation] 마지막 {len(data)}건 완료")

        cur.close()
        conn.close()

    # ── Task 14: attribute_frame_annotation_insert ────────────────────────────
    @task
    def attribute_frame_annotation_insert():
        """
        frame_annotation_new의 attribute_tokens JSON 배열을 펼쳐
        attribute_frame_annotation 연결 테이블에 INSERT한다.
        frame_annotation_insert 완료 후 실행
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            f"SELECT token, attribute_tokens FROM {STAGING_DB}.frame_annotation_new "
            "WHERE attribute_tokens IS NOT NULL AND attribute_tokens != 'null'"
        )
        rows = cur.fetchall()
        print(f"[attr_fa] attribute_tokens 있는 frame_annotation_new: {len(rows)}건")

        afa_rows = []
        for fa_token, attr_tokens_json in rows:
            try:
                tokens = json.loads(attr_tokens_json)
            except Exception:
                continue
            for attr_token in tokens:
                afa_rows.append((attr_token, fa_token))

        if afa_rows:
            for i in range(0, len(afa_rows), BATCH_SIZE):
                batch = afa_rows[i : i + BATCH_SIZE]
                cur.executemany(
                    f"INSERT IGNORE INTO {STAGING_DB}.attribute_frame_annotation "
                    "(attribute_token, frame_annotation_token) VALUES (%s, %s)",
                    batch,
                )
                conn.commit()
                print(f"[attr_fa] {min(i + BATCH_SIZE, len(afa_rows))}/{len(afa_rows)} 완료")
        else:
            print("[attr_fa] INSERT 대상 없음")

        cur.close()
        conn.close()

    # ── Task 15: scenario_insert ──────────────────────────────────────────────
    @task
    def scenario_insert():
        """
        더미 scenario 1건("lyft_level5")을 생성하고
        staging DB의 모든 scene에 scenarios_in_scene 연결 레코드를 INSERT한다.
        - adds.scenario에서 동일 description 기존 여부 확인 후 token 재사용 or 신규 생성
        scene_insert 완료 후 실행 (scene_token 필요)
        """
        conn = _get_conn()
        cur = conn.cursor()

        description = DATASET_NAME  # "lyft_level5"

        # adds.scenario에서 중복 체크
        cur.execute(
            f"SELECT token FROM {ADDS_DB}.scenario WHERE description = %s LIMIT 1",
            (description,),
        )
        result = cur.fetchone()

        if result:
            scenario_token = result[0]
            print(f"[scenario] 기존 token 재사용: {scenario_token}")
        else:
            scenario_token = secrets.token_hex(16)
            cur.execute(
                f"INSERT IGNORE INTO {MASTER_WRITE_DB}.scenario (token, description) "
                "VALUES (%s, %s)",
                (scenario_token, description),
            )
            print(f"[scenario] 신규 생성: {scenario_token}")

        # staging DB의 모든 scene_token 조회
        cur.execute(f"SELECT token FROM {STAGING_DB}.scene")
        scene_tokens = [row[0] for row in cur.fetchall()]

        sis_rows = [(scene_token, scenario_token) for scene_token in scene_tokens]
        cur.executemany(
            f"INSERT IGNORE INTO {MASTER_WRITE_DB}.scenarios_in_scene "
            "(scene_token, scenario_token) VALUES (%s, %s)",
            sis_rows,
        )
        conn.commit()
        print(f"[scenario] {len(sis_rows)}건 scenarios_in_scene 연결 완료 → {MASTER_WRITE_DB}")

        cur.close()
        conn.close()

    # ── Task 16: copy_to_adds ─────────────────────────────────────────────────
    @task
    def copy_to_adds():
        """
        staging DB(lyft_level5) → adds DB 복사.
        [테스트 중 비활성화 — 검증 완료 후 주석 해제]
        """
        print("[skip] copy_to_adds 는 현재 비활성화 상태입니다. 검증 완료 후 주석을 해제하세요.")

        # tables = [
        #     "annotation_type",
        #     "attribute",
        #     "attribute_frame_annotation",
        #     "calibrated_sensor",
        #     "category",
        #     "dataset",
        #     "ego_pose",
        #     "frame",
        #     "frame_annotation_new",
        #     "frame_data",
        #     "instance",
        #     "log",
        #     "log_map",
        #     "map",
        #     "scene",
        #     "scenario",
        #     "scenarios_in_scene",
        #     "sensor",
        # ]
        # conn = _get_conn()
        # cur  = conn.cursor()
        # for table in tables:
        #     cur.execute(
        #         f"INSERT IGNORE INTO {ADDS_DB}.{table} "
        #         f"SELECT * FROM {STAGING_DB}.{table}"
        #     )
        #     conn.commit()
        #     print(f"[copy_to_adds] {table} 완료")
        # cur.close()
        # conn.close()

    # ── 태스크 인스턴스 생성 ──────────────────────────────────────────────────
    ann_type_t = annotation_type_insert()
    cat_map_t  = category_insert()
    attr_map_t = attribute_insert()
    sensor_t   = sensor_insert()
    cs_t       = calibrated_sensor_insert()
    log_t      = log_insert()
    map_t      = map_and_log_map_insert()
    ego_t      = ego_pose_insert()
    scene_t    = scene_insert()
    frame_t    = frame_insert()
    fd_t       = frame_data_insert()
    inst_t     = instance_insert(cat_map_t)     # cat_map_t → inst_t 자동 의존
    fa_t       = frame_annotation_insert(attr_map_t)  # attr_map_t → fa_t 자동 의존
    afa_t      = attribute_frame_annotation_insert()
    scen_t     = scenario_insert()
    copy_t     = copy_to_adds()

    # ── 의존성 그래프 ─────────────────────────────────────────────────────────
    # log → scene → frame
    log_t   >> scene_t >> frame_t
    # log 완료 후 map + log_map INSERT 가능
    log_t   >> map_t
    # sensor → calibrated_sensor (sensor_token FK)
    sensor_t >> cs_t
    # frame + calibrated_sensor + ego_pose → frame_data
    [frame_t, cs_t, ego_t] >> fd_t
    # annotation_type + frame + instance → frame_annotation
    ann_type_t >> fa_t
    frame_t    >> fa_t
    inst_t     >> fa_t
    # frame_annotation → attribute_frame_annotation
    fa_t >> afa_t
    # scene → scenario
    scene_t >> scen_t
    # 모두 완료 후 copy_to_adds
    [afa_t, scen_t, map_t, fd_t, ann_type_t] >> copy_t
