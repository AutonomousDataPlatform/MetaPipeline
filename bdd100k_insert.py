"""
bdd100k_insert.py

BDD100K detection 데이터셋(det_v2_val_release.json)을 staging DB(bdd100k)에
적재한 뒤 통합 DB(adds)로 복사하는 Airflow DAG.

설계 문서: airflow/docs/bdd100k_etl_mapping.md
참고 DAG:  auto_labeling_insert.py

"""

import json
import secrets
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook


KST = timezone(timedelta(hours=9))

# ── 상수 ──────────────────────────────────────────────────────────────────────
SOURCE_FILE   = "/home/ubuntu/airflow/docs/det_v2_val_release.json"
STAGING_DB    = "bdd100k"
ADDS_DB       = "adds"
MYSQL_CONN_ID = "aws_mysql"

DATASET_NAME         = "BDD100K"
SENSOR_CHANNEL       = "BDD_CAM"
SENSOR_MODALITY      = "camera"
ANNOTATION_TYPE_NAME = "bbox_image2d"

# ── 테스트 모드 설정 ──────────────────────────────────────────────────────────
# 테스트 중: 공유 마스터(category/attribute/scenario)도 staging DB에 씀.
# 검증 완료 후 MASTER_WRITE_DB = ADDS_DB 로 변경.
MASTER_WRITE_DB = STAGING_DB

BATCH_SIZE = 50_000

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


# ── DAG 정의 ──────────────────────────────────────────────────────────────────

default_args = {
    "owner": "eun",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bdd100k_insert",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule_interval=None,
    catchup=False,
    tags=["bdd100k", "etl"],
) as dag:

    # ── Task 1: category_insert ────────────────────────────────────────────────
    @task
    def category_insert():
        """
        CATEGORY_MAP의 13개 카테고리를 처리한다.
        - adds.category(읽기)에서 기존 {name: token} 맵 생성
        - 없는 항목만 신규 생성 후 MASTER_WRITE_DB.category에 INSERT
        - {adds_name: token} 맵을 XCom으로 반환
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(f"SELECT name, token FROM {ADDS_DB}.category")
        name_token_map = {name: token for name, token in cur.fetchall()}
        print(f"[category] 기존 {ADDS_DB}.category 조회: {len(name_token_map)}건")

        new_rows = []
        for adds_name in CATEGORY_MAP.values():
            if adds_name not in name_token_map:
                token = secrets.token_hex(16)
                name_token_map[adds_name] = token
                new_rows.append((token, adds_name, None, None, None))

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
        return name_token_map

    # ── Task 2: sensor_insert ──────────────────────────────────────────────────
    @task
    def sensor_insert():
        """
        BDD_CAM 단일 카메라 센서 1건을 staging DB에 INSERT한다.
        BDD100K에 공식 채널명 없으므로 BDD_CAM으로 임의 지정.
        """
        conn = _get_conn()
        cur = conn.cursor()

        sensor_token = secrets.token_hex(16)
        cur.execute(
            f"INSERT IGNORE INTO {STAGING_DB}.sensor (token, channel, modality) "
            "VALUES (%s, %s, %s)",
            (sensor_token, SENSOR_CHANNEL, SENSOR_MODALITY),
        )
        conn.commit()
        print(f"[sensor] token={sensor_token}, channel={SENSOR_CHANNEL}")

        cur.close()
        conn.close()
        return sensor_token

    # ── Task 3: calibrated_sensor_insert ──────────────────────────────────────
    @task
    def calibrated_sensor_insert(sensor_token: str):
        """
        단일 카메라용 calibrated_sensor 1건을 staging DB에 INSERT한다.
        BDD100K에 캘리브레이션 정보가 없으므로 translation/rotation/intrinsic 모두 NULL.
        """
        conn = _get_conn()
        cur = conn.cursor()

        cs_token = secrets.token_hex(16)
        cur.execute(
            f"INSERT IGNORE INTO {STAGING_DB}.calibrated_sensor "
            "(token, sensor_token, translation, rotation, camera_intrinsic) "
            "VALUES (%s, %s, %s, %s, %s)",
            (cs_token, sensor_token, None, None, None),
        )
        conn.commit()
        print(f"[calibrated_sensor] token={cs_token}, sensor_token={sensor_token}")

        cur.close()
        conn.close()
        return cs_token

    # ── Task 4: attribute_insert ───────────────────────────────────────────────
    @task
    def attribute_insert():
        """
        trafficlight color 3가지(G/Y/R)를 처리한다.
        - adds.attribute(읽기)에서 기존 항목 조회
        - 없는 항목만 신규 생성 후 MASTER_WRITE_DB.attribute에 INSERT
        - {attribute_name: token} 맵을 XCom으로 반환
        """
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(f"SELECT name, token FROM {ADDS_DB}.attribute")
        name_token_map = {name: token for name, token in cur.fetchall()}
        print(f"[attribute] 기존 {ADDS_DB}.attribute 조회: {len(name_token_map)}건")

        new_rows = []
        for attr_name in TRAFFIC_LIGHT_COLOR_MAP.values():
            if attr_name not in name_token_map:
                token = secrets.token_hex(16)
                name_token_map[attr_name] = token
                new_rows.append((token, attr_name, None))

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
        return {name: name_token_map[name] for name in TRAFFIC_LIGHT_COLOR_MAP.values()}

    # ── Task 5: dataset_log_scene_frame_insert ────────────────────────────────
    @task
    def dataset_log_scene_frame_insert(calibrated_sensor_token: str):
        """
        dataset 1건 + videoName당 log/scene/frame/frame_data 각 1건씩 INSERT한다.
        총 10,000 videoName × 4 테이블 = 40,000건.

        BDD100K 제약:
        - log: device/date/location/driving_distance/device_wlh 모두 NULL
        - scene: base_path/goal_ego_pose_token 모두 NULL
        - frame_data: ego_pose_token/height/width 모두 NULL
        - frame.prev/next, frame_data.prev/next: 1프레임이므로 NULL
        """
        data = _load_source()
        conn = _get_conn()
        cur = conn.cursor()

        # dataset
        dataset_token = secrets.token_hex(16)
        cur.execute(
            f"INSERT IGNORE INTO {STAGING_DB}.dataset (token, name) VALUES (%s, %s)",
            (dataset_token, DATASET_NAME),
        )

        log_rows        = []
        scene_rows      = []
        frame_rows      = []
        frame_data_rows = []

        for item in data:
            log_token   = secrets.token_hex(16)
            scene_token = secrets.token_hex(16)
            frame_token = secrets.token_hex(16)
            fd_token    = secrets.token_hex(16)

            # log
            log_rows.append((
                log_token, dataset_token,
                None,               # device
                None,               # date_captured
                None,               # location
                None,               # driving_distance
                None,               # device_wlh
                item["videoName"],  # logfile
            ))

            # scene: 1프레임이므로 first_frame_token == last_frame_token
            scene_rows.append((
                scene_token, log_token,
                None,         # base_path
                1,            # num_frames
                frame_token,  # first_frame_token
                frame_token,  # last_frame_token
                None,         # goal_ego_pose_token
            ))

            # frame
            frame_rows.append((
                frame_token,
                item["timestamp"],
                scene_token,
                None,   # prev
                None,   # next
            ))

            # frame_data
            frame_data_rows.append((
                fd_token,
                frame_token,
                calibrated_sensor_token,
                None,               # ego_pose_token (없음)
                item["timestamp"],
                1,                  # is_key_frame
                item["name"],       # file_name (이후 JOIN 기준)
                "jpg",              # file_format
                None,               # height
                None,               # width
                None,               # prev
                None,               # next
            ))

        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.log "
            "(token, dataset_token, device, date_captured, location, "
            " driving_distance, device_wlh, logfile) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            log_rows,
        )
        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.scene "
            "(token, log_token, base_path, num_frames, "
            " first_frame_token, last_frame_token, goal_ego_pose_token) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            scene_rows,
        )
        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.frame "
            "(token, timestamp, scene_token, prev, next) "
            "VALUES (%s, %s, %s, %s, %s)",
            frame_rows,
        )
        cur.executemany(
            f"INSERT IGNORE INTO {STAGING_DB}.frame_data "
            "(token, frame_token, calibrated_sensor_token, ego_pose_token, "
            " timestamp, is_key_frame, file_name, file_format, "
            " height, width, prev, next) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            frame_data_rows,
        )
        conn.commit()
        print(f"[dataset_log_scene_frame] dataset_token={dataset_token}, {len(data)}건 처리")

        cur.close()
        conn.close()

    # ── Task 6: instance_frame_annotation_insert ──────────────────────────────
    @task
    def instance_frame_annotation_insert(
        category_token_map: dict,
        attribute_token_map: dict,
    ):
        """
        label 1건당 instance 1건 + frame_annotation_new 1건을 INSERT한다.
        총 186,033건을 BATCH_SIZE(50,000)씩 배치 처리.

        - frame_data_token: staging DB의 frame_data 테이블에서 file_name으로 조회
        - visibility_token: occluded=True → "0", False → "5" (adds.visibility 기존 데이터)
        - attribute_tokens: traffic light의 G/Y/R 색상 token JSON 배열 (NA 제외)
        """
        data = _load_source()
        conn = _get_conn()
        cur = conn.cursor()

        # file_name → (fd_token, frame_token) 맵 조회
        cur.execute(
            f"SELECT file_name, token, frame_token FROM {STAGING_DB}.frame_data"
        )
        fd_map = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        print(f"[instance/fa] frame_data 맵: {len(fd_map)}건")

        instance_rows = []
        fa_rows       = []

        for item in data:
            file_name = item["name"]
            if file_name not in fd_map:
                print(f"[경고] frame_data 없음: {file_name}")
                continue
            fd_token, frame_token = fd_map[file_name]

            for label in item["labels"]:
                adds_category = CATEGORY_MAP.get(label["category"])
                if not adds_category:
                    print(f"[경고] 알 수 없는 category: {label['category']}")
                    continue

                category_token = category_token_map.get(adds_category)
                if not category_token:
                    print(f"[경고] category_token 없음: {adds_category}")
                    continue

                instance_token = secrets.token_hex(16)
                fa_token       = secrets.token_hex(16)

                # occluded → visibility_token
                visibility_token = "0" if label["attributes"].get("occluded") else "5"

                # traffic light 색상만 attribute_tokens에 포함 (NA 제외)
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
                    None,                                                       # is_lidar_synced
                    None,                                                       # size_wlh
                    None,                                                       # translation_center
                    None,                                                       # rotation_orientation
                    None,                                                       # bbox_image3d
                    bbox_image2d,
                    None,                                                       # num_lidar_pts
                    None,                                                       # num_radar_pts
                    json.dumps(attr_token_list) if attr_token_list else None,   # attribute_tokens
                    None,                                                       # frame_aligned
                    None,                                                       # description
                    None,                                                       # prev
                    None,                                                       # next
                    None,                                                       # velocity
                ))

        instance_q = (
            f"INSERT IGNORE INTO {STAGING_DB}.instance "
            "(token, category_token, first_frame_annotation_token, "
            " last_frame_annotation_token, num_annotations, size) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        fa_q = (
            f"INSERT IGNORE INTO {STAGING_DB}.frame_annotation_new "
            "(token, frame_token, frame_data_token, instance_token, "
            " visibility_token, annotation_type_name, is_lidar_synced, "
            " size_wlh, translation_center, rotation_orientation, "
            " bbox_image3d, bbox_image2d, num_lidar_pts, num_radar_pts, "
            " attribute_tokens, frame_aligned, description, prev, next, velocity) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        for i in range(0, len(instance_rows), BATCH_SIZE):
            batch_end = min(i + BATCH_SIZE, len(instance_rows))
            cur.executemany(instance_q, instance_rows[i:batch_end])
            cur.executemany(fa_q, fa_rows[i:batch_end])
            conn.commit()
            print(f"[instance/fa] {batch_end}/{len(instance_rows)} 완료")

        cur.close()
        conn.close()
        print(f"[instance/fa] 완료: instance {len(instance_rows)}건, fa {len(fa_rows)}건")

    # ── Task 7: attribute_frame_annotation_insert ─────────────────────────────
    @task
    def attribute_frame_annotation_insert():
        """
        traffic light 색상 attribute가 있는 frame_annotation_new에 대해
        attribute_frame_annotation 연결 테이블을 INSERT한다.
        최대 18,529건 (G/Y/R 색상 있는 traffic light 한정).
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
            cur.executemany(
                f"INSERT IGNORE INTO {STAGING_DB}.attribute_frame_annotation "
                "(attribute_token, frame_annotation_token) VALUES (%s, %s)",
                afa_rows,
            )
            conn.commit()
            print(f"[attr_fa] {len(afa_rows)}건 INSERT 완료")
        else:
            print("[attr_fa] INSERT 대상 없음")

        cur.close()
        conn.close()

    # ── Task 8: scenario_insert ────────────────────────────────────────────────
    @task
    def scenario_insert():
        """
        frame 단위 weather+timeofday+scene 조합으로 scenario를 생성한다.

        - adds.scenario 기존 데이터는 조회하지 않음 (BDD100K 전용, 분리)
        - BDD100K 처리 내부에서만 중복 체크 (동일 description → token 재사용)
        - 최대 조합 수: 7×4×7 = 196가지
        - scenarios_in_scene: scene_token은 staging DB frame_data JOIN으로 조회
        """
        data = _load_source()
        conn = _get_conn()
        cur = conn.cursor()

        # file_name → scene_token 조회 (frame_data → frame → scene JOIN)
        cur.execute(
            f"SELECT fd.file_name, s.token "
            f"FROM {STAGING_DB}.frame_data fd "
            f"JOIN {STAGING_DB}.frame f ON fd.frame_token = f.token "
            f"JOIN {STAGING_DB}.scene s ON f.scene_token = s.token"
        )
        filename_scene_map = {row[0]: row[1] for row in cur.fetchall()}
        print(f"[scenario] file_name→scene_token 맵: {len(filename_scene_map)}건")

        local_desc_token    = {}   # description → scenario_token (내부 중복 방지)
        scenario_rows       = []
        sis_rows            = []   # scenarios_in_scene

        for item in data:
            scene_token = filename_scene_map.get(item["name"])
            if not scene_token:
                print(f"[경고] scene_token 없음: {item['name']}")
                continue

            attrs = item["attributes"]
            desc  = f"{attrs['weather']},{attrs['timeofday']},{attrs['scene']}"

            if desc not in local_desc_token:
                s_token = secrets.token_hex(16)
                local_desc_token[desc] = s_token
                scenario_rows.append((s_token, desc))

            sis_rows.append((scene_token, local_desc_token[desc]))

        if scenario_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {MASTER_WRITE_DB}.scenario "
                "(token, description) VALUES (%s, %s)",
                scenario_rows,
            )
        if sis_rows:
            cur.executemany(
                f"INSERT IGNORE INTO {MASTER_WRITE_DB}.scenarios_in_scene "
                "(scene_token, scenario_token) VALUES (%s, %s)",
                sis_rows,
            )
        conn.commit()
        print(
            f"[scenario] {len(scenario_rows)}개 시나리오 생성, "
            f"{len(sis_rows)}건 scenarios_in_scene 연결 → {MASTER_WRITE_DB}"
        )

        cur.close()
        conn.close()

    # ── Task 9: copy_to_adds ───────────────────────────────────────────────────
    @task
    def copy_to_adds():
        """
        staging DB(bdd100k) → adds DB 복사.
        [테스트 중 비활성화 — 검증 완료 후 주석 해제]
        """
        print("[skip] copy_to_adds 는 현재 비활성화 상태입니다. 검증 완료 후 주석을 해제하세요.")

        # tables = [
        #     "sensor",
        #     "calibrated_sensor",
        #     "category",
        #     "attribute",
        #     "dataset",
        #     "log",
        #     "scene",
        #     "frame",
        #     "frame_data",
        #     "instance",
        #     "frame_annotation_new",
        #     "attribute_frame_annotation",
        #     "scenario",
        #     "scenarios_in_scene",
        # ]
        # conn = _get_conn()
        # cur  = conn.cursor()
        # for table in tables:
        #     cur.execute(
        #         f"INSERT IGNORE INTO {ADDS_DB}.{table} SELECT * FROM {STAGING_DB}.{table}"
        #     )
        #     conn.commit()
        #     print(f"[copy_to_adds] {table} 완료")
        # cur.close()
        # conn.close()


    cat_map_t  = category_insert()
    sensor_t   = sensor_insert()
    cs_t       = calibrated_sensor_insert(sensor_t)
    attr_map_t = attribute_insert()

    frame_t = dataset_log_scene_frame_insert(cs_t)

    fa_t   = instance_frame_annotation_insert(cat_map_t, attr_map_t)
    afa_t  = attribute_frame_annotation_insert()
    scen_t = scenario_insert()
    copy_t = copy_to_adds()

    # frame_data가 생성된 후 instance/fa INSERT 가능
    frame_t >> fa_t
    # fa INSERT 후 attribute_frame_annotation INSERT 가능
    fa_t >> afa_t
    # frame_data가 생성된 후 scenario INSERT 가능
    frame_t >> scen_t
    # 모두 완료 후 copy_to_adds
    [afa_t, scen_t] >> copy_t
