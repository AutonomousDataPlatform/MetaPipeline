"""
ONCE Dataset → once staging DB 적재 후 adds(통합) DB 복사

구조:
  setup_global       : category / sensor / dataset / annotation_type 1회 삽입
  process_sequence   : 시퀀스별 전체 삽입 (Dynamic Task Mapping, 병렬)
  copy_to_adds       : once staging DB → adds DB 복사

── 실행 전 ──────────────────────────────────────────────────────────────────
staging DB를 먼저 비울 것:  python db/scripts/once_truncate.py
모든 INSERT가 INSERT IGNORE라 이전 실행 데이터가 남아 있으면 갱신되지 않는다.

── 공유 마스터 처리 ─────────────────────────────────────────────────────────
dataset / category / sensor / scenario 는 adds DB를 먼저 조회해서
- 이미 있으면 → adds의 token을 그대로 재사용해 staging에 기록
- 없으면      → 결정론적 신규 token 발급
재사용한 행은 copy_to_adds 시 PK 충돌로 IGNORE되어 adds에 중복되지 않는다.
attribute는 ONCE에 해당 데이터가 없어(annos = names/boxes_3d/boxes_2d) 대상이 아니다.
annotation_type은 PK가 name이라 INSERT IGNORE만으로 중복이 차단된다.

경로 규칙:
  scene.base_path        = "once/data/{sequence_id}/"
  frame_data.file_name   = "{channel}/{frame_id}.jpg"  (카메라)
                         = "lidar_roof/{frame_id}.bin" (LiDAR)
  → 결합 시 "once/data/000027/cam05/1616535381199.jpg" (ONCE 원본 배포 구조)

토큰 전략:
  - 모든 token은 sequence_id 기반 MD5로 결정론적 생성 → 재실행 안전

TODO (미완):
  - frame_annotation_new.frame_data_token : NULL → pcd 수집 후 연결
  - frame_annotation_new.bbox_image2d : annos.boxes_2d 미사용
  - instance : pseudo-instance (track_id 없음) → num_annotations=1 고정
"""

import datetime as dt
import glob
import hashlib
import json
import os
from datetime import timedelta, timezone

import numpy as np
from scipy.spatial.transform import Rotation

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ─────────────────────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────────────────────
ONCE_BASE  = "/home/ubuntu/airflow/dataset/once"

STAGING_DB = "once"
ADDS_DB    = "adds"
CONN_ID    = "aws_mysql"

DATASET_NAME = "ONCE Dataset"
# 신규 token 발급 시 데이터셋 스코프 (타 데이터셋과 token 충돌 방지)
TOKEN_SCOPE  = "once"

CAMERA_CHANNELS = ["cam01", "cam03", "cam05", "cam06", "cam07", "cam08", "cam09"]
LIDAR_CHANNEL   = "lidar_roof"

# ── copy_to_adds 대상 테이블 (참조 순서) ─────────────────────────────────────
# 제외:
#   dataset              → adds.dataset 에 인덱스가 아예 없어 INSERT IGNORE 무효.
#                          copy_to_adds 에서 NOT EXISTS 로 따로 처리한다.
#   frame_annotation_new → adds 쪽에 PK/UNIQUE 가 없어 INSERT IGNORE 가 중복을
#                          막지 못한다. copy_to_adds 에서 건수 가드 후 처리한다.
#   attribute / attribute_frame_annotation / visibility / map / log_map /
#   lidarseg             → ONCE 에 데이터 없음 (staging 0건)
COPY_TABLES = [
    "annotation_type",
    "category",
    "sensor",
    "calibrated_sensor",
    "log",
    "scene",
    "scenario",
    "scenarios_in_scene",
    "ego_pose",
    "frame",
    "frame_data",
    "instance",
]

ONCE_CATEGORY_MAP = {
    "Car":        "vehicle.car",
    "Pedestrian": "pedestrian",
    "Cyclist":    "vehicle.bicycle",
    "Truck":      "vehicle.truck",
    "Bus":        "vehicle.bus",
}

default_args = {
    "owner": "once_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────
# 공통 헬퍼
# ─────────────────────────────────────────────────────────────
def _get_conn():
    hook = MySqlHook.get_hook(conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def _parse_cam_to_velo(cam_to_velo: list) -> tuple:
    """4×4 cam_to_velo 행렬 → (translation [tx,ty,tz], rotation [w,x,y,z])"""
    m = np.array(cam_to_velo, dtype=np.float64)
    translation = m[:3, 3].tolist()
    x, y, z, w = Rotation.from_matrix(m[:3, :3]).as_quat()
    return translation, [float(w), float(x), float(y), float(z)]


def _parse_pose(pose: list) -> tuple:
    """ONCE pose [qx,qy,qz,qw,tx,ty,tz] → (translation [tx,ty,tz], rotation [w,x,y,z])"""
    qx, qy, qz, qw = pose[0], pose[1], pose[2], pose[3]
    tx, ty, tz     = pose[4], pose[5], pose[6]
    return [float(tx), float(ty), float(tz)], [float(qw), float(qx), float(qy), float(qz)]


def _yaw_to_quat(yaw_rad: float) -> list:
    """yaw (z축 회전각, rad) → quaternion [w,x,y,z]"""
    x, y, z, w = Rotation.from_euler("z", yaw_rad).as_quat()
    return [float(w), float(x), float(y), float(z)]


def _frame_id_to_date(frame_id: str) -> str:
    """ONCE frame_id (ms 문자열) → 'YYYY-MM-DD'"""
    ts = int(frame_id) / 1_000
    return dt.datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _get_sequence_ids() -> list:
    """once 폴더의 JSON 파일 목록에서 시퀀스 ID 추출"""
    paths = sorted(glob.glob(f"{ONCE_BASE}/*/*.json"))
    return [os.path.basename(p).replace(".json", "") for p in paths]


# ─────────────────────────────────────────────────────────────
# 태스크 1: setup_global
# ─────────────────────────────────────────────────────────────
def setup_global(**kwargs):
    """
    전체 공통 데이터 1회 삽입 (adds 기준 get_or_create):
    - annotation_type : bbox_pcd3d (PK=name → INSERT IGNORE로 중복 자동 차단)
    - category        : adds.category 조회 → 있으면 token 재사용, 없으면 신규
    - sensor          : adds.sensor 조회 → 있으면 token 재사용, 없으면 신규 (카메라 7 + LiDAR 1)
    - dataset         : adds.dataset 조회 → 있으면 token 재사용, 없으면 신규

    staging DB는 실행 전 truncate된 상태를 전제하므로 staging 조회는 하지 않는다.
    재사용한 token은 copy_to_adds에서 PK 충돌로 IGNORE되어 adds에 중복되지 않는다.
    """
    conn = _get_conn()
    cur  = conn.cursor()

    # ── annotation_type ──
    cur.execute(
        f"INSERT IGNORE INTO {STAGING_DB}.annotation_type (name, description) VALUES (%s, %s)",
        ("bbox_pcd3d", "3D bounding box in point cloud"),
    )

    # ── category : adds 우선 조회 ──
    cur.execute(f"SELECT name, token FROM {ADDS_DB}.category")
    adds_category = dict(cur.fetchall())
    print(f"[category] 기존 {ADDS_DB}.category: {len(adds_category)}건")

    cat_rows = []
    for orig_name, adds_name in ONCE_CATEGORY_MAP.items():
        token = adds_category.get(adds_name)
        if token:
            print(f"  [category] adds 재사용: '{adds_name}'")
        else:
            token = _md5(f"category:{TOKEN_SCOPE}:{adds_name}")
            print(f"  [category] 신규: '{adds_name}'")
        cat_rows.append((token, adds_name, None))

    cur.executemany(
        f"INSERT IGNORE INTO {STAGING_DB}.category (token, name, description) "
        f"VALUES (%s, %s, %s)",
        cat_rows,
    )

    # ── sensor : adds 우선 조회 ──
    # adds.sensor 는 channel 에 UNIQUE 가 없고 collation 이 utf8mb4_0900_ai_ci
    # (대소문자 무시) 다. 그냥 channel = 'cam01' 로 조회하면 타 데이터셋의 'CAM01'
    # 이 매칭되어, 서로 다른 차량·다른 calib 의 물리 센서가 한 token 을 공유하게 된다.
    # → BINARY 비교로 표기가 정확히 같은 행만 재사용한다.
    all_channels = CAMERA_CHANNELS + [LIDAR_CHANNEL]
    placeholders = ", ".join(["%s"] * len(all_channels))
    cur.execute(
        f"SELECT channel, token FROM {ADDS_DB}.sensor "
        f"WHERE BINARY channel IN ({placeholders})",
        all_channels,
    )
    adds_sensor = dict(cur.fetchall())

    sensor_rows = []
    for channel in all_channels:
        modality = "lidar" if channel == LIDAR_CHANNEL else "camera"
        token = adds_sensor.get(channel)
        if token:
            print(f"  [sensor] adds 재사용: '{channel}'")
        else:
            token = _md5(f"sensor:{TOKEN_SCOPE}:{channel}")
            print(f"  [sensor] 신규: '{channel}'")
        sensor_rows.append((token, channel, modality))

    cur.executemany(
        f"INSERT IGNORE INTO {STAGING_DB}.sensor (token, channel, modality) "
        f"VALUES (%s, %s, %s)",
        sensor_rows,
    )

    # ── dataset ──
    # adds.dataset 에는 인덱스가 아예 없어 INSERT IGNORE 가 통하지 않는다.
    # 여기서 name 기준으로 token 을 확정하고, 실제 삽입은 copy_to_adds 의
    # NOT EXISTS 로 중복을 막는다.
    cur.execute(
        f"SELECT token FROM {ADDS_DB}.dataset WHERE name = %s LIMIT 1",
        (DATASET_NAME,),
    )
    row = cur.fetchone()
    if row:
        dataset_token = row[0]
        print(f"  [dataset] adds 재사용: {dataset_token[:8]}...")
    else:
        dataset_token = _md5(f"dataset:{DATASET_NAME}")
        print(f"  [dataset] 신규: {dataset_token[:8]}...")

    # once.dataset 에도 PK/UNIQUE 가 없어 INSERT IGNORE 로 중복이 막히지 않는다.
    # (truncate 없이 재실행하면 'ONCE Dataset' 이 2행이 되고, copy_to_adds 의
    #  NOT EXISTS 를 두 행이 함께 통과해 adds 에도 2행이 들어간다.)
    cur.execute(
        f"SELECT token FROM {STAGING_DB}.dataset WHERE name = %s LIMIT 1",
        (DATASET_NAME,),
    )
    if cur.fetchone():
        print(f"  [dataset] staging 이미 존재 → 삽입 스킵")
    else:
        cur.execute(
            f"INSERT INTO {STAGING_DB}.dataset (token, name) VALUES (%s, %s)",
            (dataset_token, DATASET_NAME),
        )

    conn.commit()
    conn.close()
    print(f"[완료] setup_global: category {len(cat_rows)}개 / sensor {len(sensor_rows)}개 / dataset 1개")


# ─────────────────────────────────────────────────────────────
# 태스크 2: process_sequence
# ─────────────────────────────────────────────────────────────
def process_sequence(sequence_id: str, **kwargs):
    """
    시퀀스 1개 전체 삽입 순서:
      log → ego_pose → scene(NULL) → frame → scene UPDATE
      → calibrated_sensor → frame_data
      → scenario + scenarios_in_scene
      → instance → frame_annotation_new
    """
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE {STAGING_DB}")

    # ── 이미 처리된 시퀀스 스킵 ──
    log_token = _md5(f"log:{sequence_id}")
    cur.execute("SELECT token FROM log WHERE token = %s", (log_token,))
    if cur.fetchone():
        print(f"[SKIP] {sequence_id} 이미 처리됨")
        conn.close()
        return

    # ── JSON 로드 ──
    json_path = f"{ONCE_BASE}/{sequence_id}/{sequence_id}.json"
    with open(json_path, "r") as f:
        data = json.load(f)

    meta       = data["meta_info"]
    calib      = data["calib"]
    all_frames = data["frames"]
    annotated  = [fr for fr in all_frames if "annos" in fr]

    print(f"[{sequence_id}] 전체 {len(all_frames)}프레임 / annotated {len(annotated)}프레임")

    # ── 공통 token DB 조회 ──
    cur.execute("SELECT name, token FROM category")
    cat_name_to_token = dict(cur.fetchall())

    cur.execute("SELECT channel, token FROM sensor")
    sensor_token_map = dict(cur.fetchall())

    cur.execute("SELECT token FROM dataset WHERE name = %s", (DATASET_NAME,))
    row = cur.fetchone()
    dataset_token = row[0] if row else _md5(f"dataset:{DATASET_NAME}")

    # ── log ──
    date_captured = _frame_id_to_date(annotated[0]["frame_id"])
    cur.execute(
        """INSERT IGNORE INTO log
           (token, dataset_token, device, date_captured, location, logfile, driving_distance)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (log_token, dataset_token, None, date_captured, None, None, None),
    )

    # ── ego_pose ──
    ego_pose_rows = []
    for fr in annotated:
        frame_id   = fr["frame_id"]
        ep_token   = _md5(f"ego_pose:{sequence_id}:{frame_id}")
        trans, rot = _parse_pose(fr["pose"])
        ego_pose_rows.append((
            ep_token, frame_id,
            json.dumps(trans), json.dumps(rot),
            None, "vehicle",
            None, None, None,
        ))

    cur.executemany(
        """INSERT IGNORE INTO ego_pose
           (token, timestamp, translation, rotation, coordinate_system,
            pose_type, velocity, acceleration, angular_rate)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        ego_pose_rows,
    )

    # ── scene (first/last_frame_token = NULL → frame 삽입 후 UPDATE) ──
    scene_token = _md5(f"scene:{sequence_id}")
    # ONCE 원본 배포 구조: data/{sequence_id}/{channel}/{frame_id}.jpg
    # frame_data.file_name 이 base_path 기준 상대경로이므로 결합하면
    # "once/data/000027/cam05/1616535381199.jpg" 가 된다.
    base_path   = f"once/data/{sequence_id}/"
    cur.execute(
        """INSERT IGNORE INTO scene
           (token, log_token, base_path, num_frames, first_frame_token, last_frame_token)
           VALUES (%s, %s, %s, %s, NULL, NULL)""",
        (scene_token, log_token, base_path, len(annotated)),
    )

    # ── frame ──
    frame_tokens = [_md5(f"frame:{sequence_id}:{fr['frame_id']}") for fr in annotated]
    frame_rows   = []
    for i, (fr, ft) in enumerate(zip(annotated, frame_tokens)):
        prev_ft = frame_tokens[i - 1] if i > 0 else None
        next_ft = frame_tokens[i + 1] if i < len(frame_tokens) - 1 else None
        frame_rows.append((ft, fr["frame_id"], scene_token, prev_ft, next_ft))

    cur.executemany(
        "INSERT IGNORE INTO frame (token, timestamp, scene_token, prev, next) VALUES (%s, %s, %s, %s, %s)",
        frame_rows,
    )

    # ── scene first/last UPDATE ──
    cur.execute(
        "UPDATE scene SET first_frame_token = %s, last_frame_token = %s WHERE token = %s",
        (frame_tokens[0], frame_tokens[-1], scene_token),
    )

    conn.commit()
    print(f"[{sequence_id}] log/ego_pose/scene/frame 완료 ({len(annotated)}프레임)")

    # ── calibrated_sensor ──
    cs_rows = []

    # 카메라 7개
    for channel in CAMERA_CHANNELS:
        cs_token   = _md5(f"cs:{sequence_id}:{channel}")
        cam_data   = calib[channel]
        trans, rot = _parse_cam_to_velo(cam_data["cam_to_velo"])
        intrinsic  = cam_data["cam_intrinsic"]
        distortion = cam_data["distortion"]
        height_from_ground = float(np.array(cam_data["cam_to_velo"])[2, 3])
        cs_rows.append((
            cs_token, sensor_token_map[channel],
            json.dumps(trans), json.dumps(rot),
            json.dumps(intrinsic), height_from_ground,
            json.dumps(distortion),
        ))

    # LiDAR : LiDAR = 차량 기준 좌표계 → 항등 변환
    lidar_cs_token = _md5(f"cs:{sequence_id}:{LIDAR_CHANNEL}")
    cs_rows.append((
        lidar_cs_token, sensor_token_map[LIDAR_CHANNEL],
        json.dumps([0.0, 0.0, 0.0]), json.dumps([1.0, 0.0, 0.0, 0.0]),
        None, None, None,
    ))

    cur.executemany(
        """INSERT IGNORE INTO calibrated_sensor
           (token, sensor_token, translation, rotation,
            camera_intrinsic, height_from_ground, distortion)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        cs_rows,
    )
    conn.commit()
    print(f"[{sequence_id}] calibrated_sensor {len(cs_rows)}개 완료")

    # ── frame_data ──
    img_height = meta["image_size"][1]   # 1020
    img_width  = meta["image_size"][0]   # 1920
    fd_rows = []

    # 카메라 7개 × annotated frame 수
    for channel in CAMERA_CHANNELS:
        cs_token  = _md5(f"cs:{sequence_id}:{channel}")
        fd_tokens = [_md5(f"fd:{sequence_id}:{channel}:{fr['frame_id']}") for fr in annotated]
        for i, (fr, ft) in enumerate(zip(annotated, fd_tokens)):
            frame_id    = fr["frame_id"]
            frame_token = frame_tokens[i]
            ego_token   = _md5(f"ego_pose:{sequence_id}:{frame_id}")
            prev_fd     = fd_tokens[i - 1] if i > 0 else None
            next_fd     = fd_tokens[i + 1] if i < len(fd_tokens) - 1 else None
            fd_rows.append((
                ft, frame_token, ego_token, cs_token,
                frame_id, 1,
                img_height, img_width,
                f"{channel}/{frame_id}.jpg", "jpg",
                prev_fd, next_fd,
            ))

    # LiDAR × annotated frame 수
    lidar_fd_tokens = [_md5(f"fd:{sequence_id}:{LIDAR_CHANNEL}:{fr['frame_id']}") for fr in annotated]
    for i, (fr, ft) in enumerate(zip(annotated, lidar_fd_tokens)):
        frame_id    = fr["frame_id"]
        frame_token = frame_tokens[i]
        ego_token   = _md5(f"ego_pose:{sequence_id}:{frame_id}")
        prev_fd     = lidar_fd_tokens[i - 1] if i > 0 else None
        next_fd     = lidar_fd_tokens[i + 1] if i < len(lidar_fd_tokens) - 1 else None
        fd_rows.append((
            ft, frame_token, ego_token, lidar_cs_token,
            frame_id, 1,
            None, None,
            f"{LIDAR_CHANNEL}/{frame_id}.bin", "bin",
            prev_fd, next_fd,
        ))

    cur.executemany(
        """INSERT IGNORE INTO frame_data
           (token, frame_token, ego_pose_token, calibrated_sensor_token,
            timestamp, is_key_frame, height, width,
            file_name, file_format, prev, next)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        fd_rows,
    )
    conn.commit()
    print(f"[{sequence_id}] frame_data {len(fd_rows)}개 완료 (카메라 {len(CAMERA_CHANNELS) * len(annotated)} + LiDAR {len(annotated)})")

    # ── scenario ──
    description = f"{meta['period']}, {meta['weather']}"

    # adds.scenario 는 description 에 인덱스가 없어 풀스캔이 발생한다.
    # (adds 약 90만행 × 시퀀스 수) — 시퀀스가 적어 실측상 문제없지만 유의할 것.
    cur.execute(
        f"SELECT token FROM {ADDS_DB}.scenario WHERE description = %s LIMIT 1",
        (description,),
    )
    row = cur.fetchone()

    if row:
        scenario_token = row[0]
        print(f"  [scenario] adds 재사용: '{description}'")
    else:
        scenario_token = _md5(f"scenario:{TOKEN_SCOPE}:{description}")
        print(f"  [scenario] 신규: '{description}'")

    cur.execute(
        "INSERT IGNORE INTO scenario (token, description) VALUES (%s, %s)",
        (scenario_token, description),
    )
    cur.execute(
        "INSERT IGNORE INTO scenarios_in_scene (scene_token, scenario_token) VALUES (%s, %s)",
        (scene_token, scenario_token),
    )
    conn.commit()

    # ── instance + frame_annotation_new ──
    instance_rows = []
    fa_rows       = []

    for fr, frame_token in zip(annotated, frame_tokens):
        frame_id = fr["frame_id"]
        names    = fr["annos"]["names"]
        boxes_3d = fr["annos"]["boxes_3d"]

        for i, (name, box) in enumerate(zip(names, boxes_3d)):
            adds_name = ONCE_CATEGORY_MAP.get(name)
            if not adds_name:
                print(f"  [경고] unknown category '{name}' → 스킵")
                continue

            cat_token = cat_name_to_token.get(adds_name)
            if not cat_token:
                print(f"  [경고] category token 없음: '{adds_name}' → 스킵")
                continue

            inst_token = _md5(f"inst:{sequence_id}:{frame_id}:{i}")
            fa_token   = _md5(f"fa:{sequence_id}:{frame_id}:{i}")

            # pseudo-instance: num_annotations=1 고정, first=last=same fa_token
            instance_rows.append((
                inst_token, cat_token,
                1,
                fa_token,
                fa_token,
            ))

            # boxes_3d: [x, y, z, length, width, height, yaw]
            trans_center = [float(box[0]), float(box[1]), float(box[2])]
            size_wlh     = [float(box[4]), float(box[3]), float(box[5])]  # [w, l, h]
            rot_ori      = _yaw_to_quat(float(box[6]))

            fa_rows.append((
                fa_token,
                frame_token,
                None,           # frame_data_token
                inst_token,
                "bbox_pcd3d",
                json.dumps(trans_center),
                json.dumps(size_wlh),
                json.dumps(rot_ori),
                None,           # num_lidar_pts
                None,           # num_radar_pts
                None,           # attribute_tokens
                None,           # velocity
                1,              # is_lidar_synced
                1,              # frame_aligned
                None,           # visibility_token
                None,           # bbox_image3d
                None,           # bbox_image2d
                None,           # description
                None,           # prev
                None,           # next
            ))

    cur.executemany(
        """INSERT IGNORE INTO instance
           (token, category_token, num_annotations,
            first_frame_annotation_token, last_frame_annotation_token)
           VALUES (%s, %s, %s, %s, %s)""",
        instance_rows,
    )

    BATCH_SIZE = 50_000
    for start in range(0, len(fa_rows), BATCH_SIZE):
        batch = fa_rows[start : start + BATCH_SIZE]
        cur.executemany(
            """INSERT IGNORE INTO frame_annotation_new
               (token, frame_token, frame_data_token, instance_token,
                annotation_type_name,
                translation_center, size_wlh, rotation_orientation,
                num_lidar_pts, num_radar_pts, attribute_tokens,
                velocity, is_lidar_synced, frame_aligned,
                visibility_token, bbox_image3d, bbox_image2d, description,
                prev, next)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            batch,
        )
        print(f"  [{sequence_id}] frame_annotation_new 배치 {start}~{start + len(batch) - 1}")

    conn.commit()
    conn.close()
    print(f"[{sequence_id}] instance {len(instance_rows)}개 / frame_annotation_new {len(fa_rows)}개 완료")


# ─────────────────────────────────────────────────────────────
# 태스크 3: copy_to_adds
# ─────────────────────────────────────────────────────────────
def copy_to_adds(**kwargs):
    """
    staging DB(once) → adds DB 복사.

    - dataset : adds.dataset 에는 PK/UNIQUE/인덱스가 하나도 없어 INSERT IGNORE 가
                통하지 않는다 → name 기준 NOT EXISTS 로 직접 중복을 막는다.
    - COPY_TABLES : 전부 PK/UNIQUE 가 있어 INSERT IGNORE 로 안전하다. 공유 마스터
                (category/sensor/scenario/annotation_type)는 setup_global 에서
                adds token 을 재사용했으므로 PK 충돌로 자동 스킵된다.
    - frame_annotation_new : adds 쪽에 PK/UNIQUE 가 없다(idx_frame_token 은
                non-unique). INSERT IGNORE 가 중복을 막지 못하므로 복사 전 건수를
                비교해 미복사(0건)일 때만 복사하고, 부분 복사 상태면 중단한다.
                롤백은 db/migrations/once_adds_rollback.sql 사용.

    두 DB 모두 FK 제약이 없어 COPY_TABLES 순서는 논리적 가독성용이다.
    """
    conn = _get_conn()
    cur  = conn.cursor()

    # ── dataset : name 기준 중복 방지 (adds.dataset 에 인덱스 없음) ──
    cur.execute(
        f"INSERT INTO {ADDS_DB}.dataset (token, name) "
        f"SELECT s.token, s.name FROM {STAGING_DB}.dataset s "
        f"WHERE NOT EXISTS ("
        f"  SELECT 1 FROM {ADDS_DB}.dataset a WHERE a.name = s.name)"
    )
    conn.commit()
    print(f"[copy_to_adds] dataset {cur.rowcount}건 복사")

    # ── PK/UNIQUE 가 있어 INSERT IGNORE 로 안전한 테이블 ──
    for table in COPY_TABLES:
        cur.execute(
            f"INSERT IGNORE INTO {ADDS_DB}.{table} "
            f"SELECT * FROM {STAGING_DB}.{table}"
        )
        conn.commit()
        print(f"[copy_to_adds] {table} {cur.rowcount}건 복사")

    # ── frame_annotation_new : 중복 가드 후 복사 ──
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_DB}.frame_annotation_new")
    staging_cnt = cur.fetchone()[0]

    # adds.frame_annotation_new.token 에는 인덱스가 없다. 풀스캔(약 6천만행)을
    # 피하려고 idx_frame_token 을 타는 frame_token 기준으로 센다. ONCE frame token
    # 은 ONCE 전용이라 이 건수가 곧 기존 복사 건수다.
    cur.execute(
        f"SELECT COUNT(*) FROM {ADDS_DB}.frame_annotation_new "
        f"WHERE frame_token IN (SELECT token FROM {STAGING_DB}.frame)"
    )
    adds_cnt = cur.fetchone()[0]

    if adds_cnt == 0:
        cur.execute(
            f"INSERT INTO {ADDS_DB}.frame_annotation_new "
            f"SELECT * FROM {STAGING_DB}.frame_annotation_new"
        )
        conn.commit()
        print(f"[copy_to_adds] frame_annotation_new {cur.rowcount}건 복사")
    elif adds_cnt == staging_cnt:
        print(f"[copy_to_adds] frame_annotation_new 이미 복사됨 ({adds_cnt}건) → 스킵")
    else:
        cur.close()
        conn.close()
        raise AirflowFailException(
            f"frame_annotation_new 부분 복사 감지: adds {adds_cnt}건 / staging {staging_cnt}건. "
            f"adds.frame_annotation_new 에는 UNIQUE 가 없어 그대로 재복사하면 중복이 발생한다. "
            f"db/migrations/once_adds_rollback.sql 로 롤백 후 재실행할 것."
        )

    cur.close()
    conn.close()
    print("[완료] copy_to_adds")


# ─────────────────────────────────────────────────────────────
# DAG 정의
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="once_insert",
    default_args=default_args,
    description="ONCE Dataset → once staging DB 삽입 후 adds DB 복사",
    schedule=None,
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=3,
    tags=["once"],
) as dag:

    t_setup = PythonOperator(
        task_id="setup_global",
        python_callable=setup_global,
    )

    @task
    def get_sequence_ids():
        ids = _get_sequence_ids()
        print(f"[get_sequence_ids] 시퀀스 {len(ids)}개 발견: {ids}")
        return [{"sequence_id": sid} for sid in ids]

    sequence_kwargs = get_sequence_ids()

    t_sequences = PythonOperator.partial(
        task_id="process_sequence",
        python_callable=process_sequence,
    ).expand(op_kwargs=sequence_kwargs)

    t_copy = PythonOperator(
        task_id="copy_to_adds",
        python_callable=copy_to_adds,
    )

    [t_setup, sequence_kwargs] >> t_sequences >> t_copy
