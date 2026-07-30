"""
bdd100k_insert.py

BDD100K detection 데이터셋(det_v2_val_release.json)을 staging DB(bdd100k)에
적재한 뒤 통합 DB(adds)로 복사하는 Airflow DAG.

설계 문서: airflow/docs/bdd100k_etl_mapping.md
참고 DAG:  once_insert.py (setup_global + copy_to_adds 2단계 구조)
           lyft_level5_insert.py (scoped_token 결정적 토큰)

실행 순서
---------
  [수동] python airflow/db/scripts/bdd100k_truncate.py   ← staging 초기화
  [DAG]  setup_master → log_scene_frame → instance/annotation → copy_to_adds

토큰 정책
---------
모든 token 은 _scoped_token() 으로 만드는 결정적 MD5 값이다.
같은 원본을 몇 번 적재해도 같은 token 이 나오므로, adds 로의 dump 를
반복해도 PRIMARY KEY 가 있는 테이블은 INSERT IGNORE 로 걸러진다.
PK 가 없는 3개 테이블(dataset / frame_annotation_new /
attribute_frame_annotation)은 copy_to_adds 에서 별도로 중복을 막는다.

공유 마스터 정책
----------------
category / attribute / sensor / calibrated_sensor / dataset / scenario 는
먼저 adds 에서 조회해 기존 token 을 재사용한다. 없을 때만 새로 만든다.
(adds.category / sensor / scenario 는 name·channel·description 에 UNIQUE 가
 없어, 조회 없이 새 token 을 만들면 adds 에 같은 이름이 중복 생성된다.)
"""

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook


KST = timezone(timedelta(hours=9))

# ── 상수 ──────────────────────────────────────────────────────────────────────
SOURCE_FILE   = "/home/ubuntu/airflow/dataset/det_v2_val_release.json"
STAGING_DB    = "bdd100k"
ADDS_DB       = "adds"
MYSQL_CONN_ID = "aws_mysql"

DATASET_NAME         = "BDD100K"
SENSOR_CHANNEL       = "BDD_CAM"
SENSOR_MODALITY      = "camera"
ANNOTATION_TYPE_NAME = "bbox_image2d"   # adds.annotation_type 에 이미 존재

# scene.base_path — 원본 배포본의 이미지 디렉터리.
# det_v2_val_release.json 은 validation split 이므로 .../100k/val/ 이다.
# adds 관례상 base_path 는 항상 슬래시로 끝나고, frame_data.file_name 과
# 이어붙이면 이미지 상대경로가 된다.
#   base_path + file_name = bdd100k/images/100k/val/b1c66a42-6f7d68ca.jpg
SCENE_BASE_PATH = "bdd100k/images/100k/val/"

# executemany 배치 크기
BATCH_SIZE       = 50_000
FRAME_BATCH_SIZE = 5_000

# ── Category 변환 맵 (BDD100K 원본 → ADDS 계층 경로) ──────────────────────────
CATEGORY_MAP = {
    "car":           "vehicle.car",
    "truck":         "vehicle.truck",
    "bus":           "vehicle.bus",
    "train":         "vehicle.train",
    "motorcycle":    "vehicle.motorcycle",
    "bicycle":       "vehicle.bicycle",
    "other vehicle": "vehicle.other_vehicle",
    "trailer":       "vehicle.trailer",
    "pedestrian":    "human.pedestrian",
    "rider":         "human.rider",
    "other person":  "human.other_person",
    "traffic light": "traffic_control.traffic_light",
    "traffic sign":  "traffic_control.traffic_sign",
}

# ── trafficLightColor 코드 → attribute name (NA 제외) ─────────────────────────
TRAFFIC_LIGHT_COLOR_MAP = {
    "G": "trafficlight.color.green",
    "Y": "trafficlight.color.yellow",
    "R": "trafficlight.color.red",
}


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────

def _get_conn():
    hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def _load_source():
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _scoped_token(value: str) -> str:
    """
    데이터셋 스코프를 붙여 만드는 결정적 32자 token.

    lyft_level5_insert.scoped_token() 과 같은 규칙.
    BDD100K 는 videoName 이 10,000개 1:1 유니크이고 label.id 가 전역 유니크
    (186,033개)라, 이 둘을 자연키로 쓰면 실행할 때마다 같은 token 이 나온다.
    """
    return hashlib.md5(f"{DATASET_NAME}:{value}".encode()).hexdigest()


def _executemany_batched(cur, conn, query, rows, batch_size, label):
    """rows 를 batch_size 씩 끊어 executemany + commit."""
    if not rows:
        print(f"  [{label}] 대상 없음")
        return
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        cur.executemany(query, chunk)
        conn.commit()
        print(f"  [{label}] {min(i + batch_size, len(rows))}/{len(rows)}")


# ── DAG 정의 ──────────────────────────────────────────────────────────────────

default_args = {
    "owner": "eun",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bdd100k_insert",
    default_args=default_args,
    description="BDD100K → bdd100k staging DB 적재 후 adds DB 복사",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule_interval=None,
    catchup=False,
    tags=["bdd100k", "etl"],
) as dag:

    # ── Task 1: setup_master ──────────────────────────────────────────────────
    @task
    def setup_master():
        """
        공유 마스터 5종을 adds 기준 get-or-create 한 뒤 staging 에 적재한다.
          dataset / category / attribute / sensor / calibrated_sensor

        adds 에 이미 있으면 그 token 을 그대로 재사용한다. 그래야 copy_to_adds
        의 INSERT IGNORE 가 PK 로 정확히 걸러내고, adds 에 같은 이름이 두 번
        생기지 않는다.

        adds 에 이미 존재하는 마스터 행은 staging 에 넣지 않는다
        (staging 은 "이번에 새로 만든 것"만 담고, copy_to_adds 가 그것만 옮긴다).
        """
        conn = _get_conn()
        cur = conn.cursor()

        # ── dataset ──
        cur.execute(
            f"SELECT token FROM {ADDS_DB}.dataset WHERE name = %s", (DATASET_NAME,)
        )
        row = cur.fetchone()
        if row:
            dataset_token = row[0]
            print(f"[dataset] adds 기존 재사용: {dataset_token}")
        else:
            dataset_token = _scoped_token("dataset")
            # dataset 은 PK 가 없어 INSERT IGNORE 가 듣지 않는다 → NOT EXISTS 로 방어
            cur.execute(
                f"INSERT INTO {STAGING_DB}.dataset (token, name) "
                f"SELECT %s, %s FROM DUAL WHERE NOT EXISTS "
                f"(SELECT 1 FROM {STAGING_DB}.dataset WHERE token = %s)",
                (dataset_token, DATASET_NAME, dataset_token),
            )
            print(f"[dataset] 신규: {dataset_token}")

        # ── category ──
        cur.execute(f"SELECT name, token FROM {ADDS_DB}.category")
        adds_category = dict(cur.fetchall())
        print(f"[category] adds 기존 {len(adds_category)}건 조회")

        category_token_map = {}
        new_category_rows = []
        for adds_name in CATEGORY_MAP.values():
            if adds_name in adds_category:
                category_token_map[adds_name] = adds_category[adds_name]
            else:
                token = _scoped_token(f"category:{adds_name}")
                category_token_map[adds_name] = token
                new_category_rows.append((token, adds_name, None, None, None))

        if new_category_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {STAGING_DB}.category "
                "(token, name, description, default_scale, `index`) "
                "VALUES (%s, %s, %s, %s, %s)",
                new_category_rows,
            )
        print(
            f"[category] 재사용 {len(CATEGORY_MAP) - len(new_category_rows)}건 / "
            f"신규 {len(new_category_rows)}건"
        )

        # ── attribute ──
        cur.execute(f"SELECT name, token FROM {ADDS_DB}.attribute")
        adds_attribute = dict(cur.fetchall())
        print(f"[attribute] adds 기존 {len(adds_attribute)}건 조회")

        attribute_token_map = {}
        new_attribute_rows = []
        for attr_name in TRAFFIC_LIGHT_COLOR_MAP.values():
            if attr_name in adds_attribute:
                attribute_token_map[attr_name] = adds_attribute[attr_name]
            else:
                token = _scoped_token(f"attribute:{attr_name}")
                attribute_token_map[attr_name] = token
                new_attribute_rows.append((token, attr_name, None))

        if new_attribute_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {STAGING_DB}.attribute "
                "(token, name, description) VALUES (%s, %s, %s)",
                new_attribute_rows,
            )
        print(
            f"[attribute] 재사용 {len(TRAFFIC_LIGHT_COLOR_MAP) - len(new_attribute_rows)}건 / "
            f"신규 {len(new_attribute_rows)}건"
        )

        # ── sensor ──
        # adds.sensor 는 channel 에 UNIQUE 가 없다. 조회 없이 만들면 중복된다.
        cur.execute(
            f"SELECT token FROM {ADDS_DB}.sensor WHERE channel = %s LIMIT 1",
            (SENSOR_CHANNEL,),
        )
        row = cur.fetchone()
        if row:
            sensor_token = row[0]
            print(f"[sensor] adds 기존 재사용: {sensor_token}")
        else:
            sensor_token = _scoped_token(f"sensor:{SENSOR_CHANNEL}")
            cur.execute(
                f"INSERT IGNORE INTO {STAGING_DB}.sensor (token, channel, modality) "
                "VALUES (%s, %s, %s)",
                (sensor_token, SENSOR_CHANNEL, SENSOR_MODALITY),
            )
            print(f"[sensor] 신규: {sensor_token} ({SENSOR_CHANNEL})")

        # ── calibrated_sensor ──
        # BDD100K 는 캘리브레이션 정보가 없어 translation/rotation/intrinsic 모두 NULL.
        cur.execute(
            f"SELECT token FROM {ADDS_DB}.calibrated_sensor "
            "WHERE sensor_token = %s LIMIT 1",
            (sensor_token,),
        )
        row = cur.fetchone()
        if row:
            cs_token = row[0]
            print(f"[calibrated_sensor] adds 기존 재사용: {cs_token}")
        else:
            cs_token = _scoped_token(f"calibrated_sensor:{SENSOR_CHANNEL}")
            cur.execute(
                f"INSERT IGNORE INTO {STAGING_DB}.calibrated_sensor "
                "(token, sensor_token, translation, rotation, camera_intrinsic) "
                "VALUES (%s, %s, %s, %s, %s)",
                (cs_token, sensor_token, None, None, None),
            )
            print(f"[calibrated_sensor] 신규: {cs_token}")

        conn.commit()
        cur.close()
        conn.close()

        return {
            "dataset_token":       dataset_token,
            "sensor_token":        sensor_token,
            "cs_token":            cs_token,
            "category_token_map":  category_token_map,
            "attribute_token_map": attribute_token_map,
        }

    # ── Task 2: log_scene_frame_insert ────────────────────────────────────────
    @task
    def log_scene_frame_insert(master: dict):
        """
        videoName 당 log / scene / frame / frame_data 각 1건씩 적재한다.
        10,000 videoName × 4 테이블 = 40,000건.

        BDD100K 제약:
        - log: device/date_captured/location/driving_distance/device_wlh 모두 NULL
        - scene: goal_ego_pose_token NULL, 1프레임이므로 first=last
                 base_path 는 SCENE_BASE_PATH 고정값
        - frame_data: ego_pose_token/height/width NULL
        - frame.prev/next, frame_data.prev/next: 1프레임이므로 NULL
        """
        data = _load_source()
        dataset_token = master["dataset_token"]
        cs_token      = master["cs_token"]

        conn = _get_conn()
        cur = conn.cursor()

        log_rows, scene_rows, frame_rows, frame_data_rows = [], [], [], []

        for item in data:
            video_name  = item["videoName"]
            log_token   = _scoped_token(f"log:{video_name}")
            scene_token = _scoped_token(f"scene:{video_name}")
            frame_token = _scoped_token(f"frame:{video_name}")
            fd_token    = _scoped_token(f"frame_data:{video_name}")

            log_rows.append((
                log_token, dataset_token,
                None,        # device
                None,        # date_captured
                None,        # location
                None,        # driving_distance
                None,        # device_wlh
                video_name,  # logfile
            ))

            scene_rows.append((
                scene_token, log_token,
                SCENE_BASE_PATH,
                1,            # num_frames
                frame_token,  # first_frame_token
                frame_token,  # last_frame_token
                None,         # goal_ego_pose_token
            ))

            frame_rows.append((
                frame_token,
                item["timestamp"],
                scene_token,
                None,   # prev
                None,   # next
            ))

            # file_name 은 "b1c66a42-6f7d68ca.jpg" 형태(확장자 포함, 경로 없음).
            # file_format 은 확장자에서 뽑아 둘이 어긋나지 않게 한다.
            file_name   = item["name"]
            file_format = os.path.splitext(file_name)[1].lstrip(".").lower()

            frame_data_rows.append((
                fd_token,
                frame_token,
                None,               # ego_pose_token (BDD100K 에 없음)
                cs_token,
                item["timestamp"],
                1,                  # is_key_frame
                None,               # height
                None,               # width
                file_name,
                file_format,
                None,               # prev
                None,               # next
            ))

        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.log "
            "(token, dataset_token, device, date_captured, location, "
            " driving_distance, device_wlh, logfile) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            log_rows, FRAME_BATCH_SIZE, "log",
        )
        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.scene "
            "(token, log_token, base_path, num_frames, "
            " first_frame_token, last_frame_token, goal_ego_pose_token) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            scene_rows, FRAME_BATCH_SIZE, "scene",
        )
        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.frame "
            "(token, timestamp, scene_token, prev, next) "
            "VALUES (%s, %s, %s, %s, %s)",
            frame_rows, FRAME_BATCH_SIZE, "frame",
        )
        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.frame_data "
            "(token, frame_token, ego_pose_token, calibrated_sensor_token, "
            " timestamp, is_key_frame, height, width, file_name, file_format, "
            " prev, next) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            frame_data_rows, FRAME_BATCH_SIZE, "frame_data",
        )

        cur.close()
        conn.close()
        print(f"[log_scene_frame] {len(data)}개 videoName × 4테이블 적재 완료")

    # ── Task 3: instance_frame_annotation_insert ──────────────────────────────
    @task
    def instance_frame_annotation_insert(master: dict):
        """
        label 1건당 instance 1건 + frame_annotation_new 1건을 적재한다. 총 186,033건.

        token 이 결정적이라 frame_data 를 조회할 필요가 없다
        (adds.frame_data 는 569만 행이라 전체 조회는 불가능하다).

        - visibility_token : occluded=True → "0"(v0), False → "5"(v100)
                             둘 다 adds.visibility 에 이미 존재
        - attribute_tokens : traffic light 의 G/Y/R 색상 token JSON 배열 (NA 제외)
        """
        data = _load_source()
        category_token_map  = master["category_token_map"]
        attribute_token_map = master["attribute_token_map"]

        conn = _get_conn()
        cur = conn.cursor()

        instance_rows, fa_rows = [], []
        skipped = 0

        for item in data:
            video_name  = item["videoName"]
            frame_token = _scoped_token(f"frame:{video_name}")
            fd_token    = _scoped_token(f"frame_data:{video_name}")

            for label in item["labels"]:
                adds_category = CATEGORY_MAP.get(label["category"])
                if not adds_category:
                    print(f"[경고] 알 수 없는 category: {label['category']}")
                    skipped += 1
                    continue

                category_token = category_token_map.get(adds_category)
                if not category_token:
                    print(f"[경고] category_token 없음: {adds_category}")
                    skipped += 1
                    continue

                label_id       = label["id"]
                instance_token = _scoped_token(f"instance:{video_name}:{label_id}")
                fa_token       = _scoped_token(f"frame_annotation:{video_name}:{label_id}")

                # occluded → visibility_token
                visibility_token = "0" if label["attributes"].get("occluded") else "5"

                # traffic light 색상만 attribute_tokens 에 포함 (NA 제외)
                attr_token_list = []
                if label["category"] == "traffic light":
                    color_code = label["attributes"]["trafficLightColor"][1]
                    attr_name  = TRAFFIC_LIGHT_COLOR_MAP.get(color_code)
                    if attr_name:
                        attr_token = attribute_token_map.get(attr_name)
                        if attr_token:
                            attr_token_list.append(attr_token)

                # box2d → [x1, y1, x2, y2] JSON
                b = label["box2d"]
                bbox_image2d = json.dumps([b["x1"], b["y1"], b["x2"], b["y2"]])

                instance_rows.append((
                    instance_token,
                    category_token,
                    fa_token,   # first_frame_annotation_token
                    fa_token,   # last_frame_annotation_token
                    1,          # num_annotations
                    None,       # size
                ))

                fa_rows.append((
                    fa_token,
                    frame_token,
                    fd_token,
                    instance_token,
                    visibility_token,
                    ANNOTATION_TYPE_NAME,
                    None,                                                      # is_lidar_synced
                    None,                                                      # size_wlh
                    None,                                                      # translation_center
                    None,                                                      # rotation_orientation
                    None,                                                      # bbox_image3d
                    bbox_image2d,
                    None,                                                      # num_lidar_pts
                    None,                                                      # num_radar_pts
                    json.dumps(attr_token_list) if attr_token_list else None,  # attribute_tokens
                    None,                                                      # frame_aligned
                    None,                                                      # description
                    None,                                                      # prev
                    None,                                                      # next
                    None,                                                      # velocity
                ))

        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.instance "
            "(token, category_token, first_frame_annotation_token, "
            " last_frame_annotation_token, num_annotations, size) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            instance_rows, BATCH_SIZE, "instance",
        )
        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.frame_annotation_new "
            "(token, frame_token, frame_data_token, instance_token, "
            " visibility_token, annotation_type_name, is_lidar_synced, "
            " size_wlh, translation_center, rotation_orientation, "
            " bbox_image3d, bbox_image2d, num_lidar_pts, num_radar_pts, "
            " attribute_tokens, frame_aligned, description, prev, next, velocity) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            fa_rows, BATCH_SIZE, "frame_annotation_new",
        )

        cur.close()
        conn.close()
        print(
            f"[instance/fa] instance {len(instance_rows)}건, "
            f"frame_annotation_new {len(fa_rows)}건 적재 (스킵 {skipped}건)"
        )

    # ── Task 4: attribute_frame_annotation_insert ─────────────────────────────
    @task
    def attribute_frame_annotation_insert():
        """
        attribute_tokens 가 있는 frame_annotation_new 를 펼쳐
        attribute_frame_annotation 연결 테이블에 적재한다.
        traffic light 의 G/Y/R 색상 한정, 최대 18,529건.
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
                if attr_token:
                    afa_rows.append((attr_token, fa_token))

        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.attribute_frame_annotation "
            "(attribute_token, frame_annotation_token) VALUES (%s, %s)",
            afa_rows, BATCH_SIZE, "attribute_frame_annotation",
        )

        cur.close()
        conn.close()
        print(f"[attr_fa] {len(afa_rows)}건 적재 완료")

    # ── Task 5: scenario_insert ───────────────────────────────────────────────
    @task
    def scenario_insert():
        """
        frame 단위 weather+timeofday+scene 조합으로 scenario 를 만든다 (97가지).

        adds.scenario 는 description 에 UNIQUE 가 없다. 조회 없이 새 token 을
        만들면 같은 description 이 계속 쌓인다(현재 adds 는 893,147행 /
        고유 description 930개). 따라서 description 으로 먼저 조회한다.
        """
        data = _load_source()

        conn = _get_conn()
        cur = conn.cursor()

        # description → token (중복 description 은 가장 작은 token 하나로 대표)
        cur.execute(
            f"SELECT description, MIN(token) FROM {ADDS_DB}.scenario "
            "GROUP BY description"
        )
        adds_scenario = dict(cur.fetchall())
        print(f"[scenario] adds 기존 고유 description {len(adds_scenario)}건 조회")

        desc_token    = {}   # description → scenario_token
        scenario_rows = []
        sis_rows      = []   # scenarios_in_scene
        reused        = 0

        for item in data:
            scene_token = _scoped_token(f"scene:{item['videoName']}")

            attrs = item["attributes"]
            desc  = f"{attrs['weather']},{attrs['timeofday']},{attrs['scene']}"

            if desc not in desc_token:
                if desc in adds_scenario:
                    desc_token[desc] = adds_scenario[desc]
                    reused += 1
                else:
                    token = _scoped_token(f"scenario:{desc}")
                    desc_token[desc] = token
                    scenario_rows.append((token, desc))

            sis_rows.append((scene_token, desc_token[desc]))

        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.scenario (token, description) "
            "VALUES (%s, %s)",
            scenario_rows, BATCH_SIZE, "scenario",
        )
        _executemany_batched(
            cur, conn,
            f"INSERT IGNORE INTO {STAGING_DB}.scenarios_in_scene "
            "(scene_token, scenario_token) VALUES (%s, %s)",
            sis_rows, FRAME_BATCH_SIZE, "scenarios_in_scene",
        )

        cur.close()
        conn.close()
        print(
            f"[scenario] 재사용 {reused}건 / 신규 {len(scenario_rows)}건, "
            f"scenarios_in_scene {len(sis_rows)}건"
        )

    # ── Task 6: copy_to_adds ──────────────────────────────────────────────────
    @task
    def copy_to_adds():
        """
        staging DB(bdd100k) → 통합 DB(adds) 복사.

        컬럼 순서가 14개 테이블 모두 일치함을 확인했으므로 SELECT * 를 쓴다.

        중복 방지:
        - PK 가 있는 11개 테이블 → INSERT IGNORE 로 걸러진다. token 이 결정적이라
          몇 번 실행해도 안전하다.
        - PK 가 없는 3개 테이블은 INSERT IGNORE 가 듣지 않으므로 별도 처리한다.
            dataset                    : NOT EXISTS (adds.dataset 은 10행이라 저렴)
            frame_annotation_new       : 대표 frame_token 1개로 적재 여부 확인
                                         (idx_frame_token 사용 → 빠름)
            attribute_frame_annotation : 대표 frame_annotation_token 1개로 확인
        """
        conn = _get_conn()
        cur = conn.cursor()

        # ── PK 있는 테이블: INSERT IGNORE (의존 순서대로) ──
        pk_tables = [
            "category",
            "attribute",
            "sensor",
            "calibrated_sensor",
            "log",
            "scene",
            "frame",
            "frame_data",
            "instance",
            "scenario",
            "scenarios_in_scene",
        ]
        for table in pk_tables:
            cur.execute(
                f"INSERT IGNORE INTO {ADDS_DB}.{table} "
                f"SELECT * FROM {STAGING_DB}.{table}"
            )
            conn.commit()
            print(f"  [copy] {table}: {cur.rowcount}행")

        # ── dataset: PK 없음 → NOT EXISTS ──
        cur.execute(
            f"INSERT INTO {ADDS_DB}.dataset (token, name) "
            f"SELECT s.token, s.name FROM {STAGING_DB}.dataset s "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {ADDS_DB}.dataset a WHERE a.token = s.token)"
        )
        conn.commit()
        print(f"  [copy] dataset: {cur.rowcount}행")

        # ── frame_annotation_new: PK 없음 → 대표 frame_token 으로 적재 여부 확인 ──
        cur.execute(
            f"SELECT frame_token FROM {STAGING_DB}.frame_annotation_new LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("  [copy] frame_annotation_new: staging 비어있음 → 건너뜀")
        else:
            cur.execute(
                f"SELECT 1 FROM {ADDS_DB}.frame_annotation_new "
                "WHERE frame_token = %s LIMIT 1",
                (row[0],),
            )
            if cur.fetchone():
                print("  [copy] frame_annotation_new: 이미 adds 에 있음 → 건너뜀")
            else:
                cur.execute(
                    f"INSERT INTO {ADDS_DB}.frame_annotation_new "
                    f"SELECT * FROM {STAGING_DB}.frame_annotation_new"
                )
                conn.commit()
                print(f"  [copy] frame_annotation_new: {cur.rowcount}행")

        # ── attribute_frame_annotation: PK 없음 → 대표 token 으로 적재 여부 확인 ──
        cur.execute(
            f"SELECT frame_annotation_token FROM "
            f"{STAGING_DB}.attribute_frame_annotation LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            print("  [copy] attribute_frame_annotation: staging 비어있음 → 건너뜀")
        else:
            cur.execute(
                f"SELECT 1 FROM {ADDS_DB}.attribute_frame_annotation "
                "WHERE frame_annotation_token = %s LIMIT 1",
                (row[0],),
            )
            if cur.fetchone():
                print("  [copy] attribute_frame_annotation: 이미 adds 에 있음 → 건너뜀")
            else:
                cur.execute(
                    f"INSERT INTO {ADDS_DB}.attribute_frame_annotation "
                    f"SELECT * FROM {STAGING_DB}.attribute_frame_annotation"
                )
                conn.commit()
                print(f"  [copy] attribute_frame_annotation: {cur.rowcount}행")

        cur.close()
        conn.close()
        print("[copy_to_adds] 완료")

    # ── 태스크 의존성 ──────────────────────────────────────────────────────────
    master_t = setup_master()
    lsf_t    = log_scene_frame_insert(master_t)
    ifa_t    = instance_frame_annotation_insert(master_t)
    afa_t    = attribute_frame_annotation_insert()
    scen_t   = scenario_insert()
    copy_t   = copy_to_adds()

    # frame/frame_data 가 있어야 instance/frame_annotation 의 참조가 성립
    lsf_t >> ifa_t
    # frame_annotation_new 적재 후 attribute_frame_annotation 생성 가능
    ifa_t >> afa_t
    # scene 이 있어야 scenarios_in_scene 연결 가능
    lsf_t >> scen_t
    # 전부 끝난 뒤 adds 로 복사
    [afa_t, scen_t] >> copy_t
