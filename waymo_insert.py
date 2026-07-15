"""
Waymo Open Dataset → waymo staging DB 전체 세그먼트 삽입

구조:
  setup_global       : category / sensor / dataset 1회 삽입
  process_segment    : 세그먼트별 전체 삽입 (Dynamic Task Mapping, 병렬)

토큰 전략:
  - 모든 token은 segment_id 기반 MD5로 결정론적 생성 → 재실행 안전
  - category / sensor / dataset 은 adds DB 조회 후 get_or_create

TODO (미완):
  - frame_data.file_name : "{channel}/{ts_micros}.jpg" (scene.base_path = "waymo/validation/{segment_id}" 기준 상대경로)
  - 실제 이미지 저장
  - ego_pose.coordinate_system : NULL → Waymo world frame EPSG 확정 후 채울 것
  - log.driving_distance / log.logfile : NULL → 필요 시 추가
  - frame_data.is_key_frame : True 고정 → camera_image parquet 정상 파일 확보 시 실제 값으로 교체
  - frame_annotation_new.frame_data_token : NULL → pcd 수집 후 연결
  - WAYMO_TYPE_TO_CATEGORY : adds.category 실데이터 확인 후 매핑 검증 필요
"""

import datetime as dt
import glob
import hashlib
import json
import os
import secrets
from datetime import timedelta, timezone

import numpy as np
import pandas as pd
from scipy.spatial.transform import Rotation

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ─────────────────────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────────────────────
WAYMO_BASE = "/home/ubuntu/airflow/dataset/waymo/validation"

STAGING_DB = "waymo"
ADDS_DB    = "adds"
CONN_ID    = "aws_mysql"

CAMERA_CHANNEL_MAP = {
    1: "FRONT",
    2: "FRONT_LEFT",
    3: "FRONT_RIGHT",
    4: "SIDE_LEFT",
    5: "SIDE_RIGHT",
}
LIDAR_CHANNEL_MAP = {
    1: "TOP",
    2: "LIDAR_FRONT",
    3: "LIDAR_SIDE_LEFT",
    4: "LIDAR_SIDE_RIGHT",
    5: "REAR",
}

# 실 운영 전 SELECT name FROM adds.category 로 반드시 확인
WAYMO_TYPE_TO_CATEGORY = {
    1: "vehicle.car",
    2: "pedestrian",
    3: "static_object.sign",
    4: "vehicle.bicycle",
}

default_args = {
    "owner": "waymo_etl",
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


def _parse_transform(flat_arr):
    """float64[16] 행렬 → (translation [tx,ty,tz], rotation [w,x,y,z])"""
    m = np.array(flat_arr, dtype=np.float64).reshape(4, 4)
    translation = m[:3, 3].tolist()
    x, y, z, w = Rotation.from_matrix(m[:3, :3]).as_quat()
    return translation, [w, float(x), float(y), float(z)]


def _heading_to_quat(heading_rad: float):
    """z축 단일 회전각(rad) → [w,x,y,z]"""
    x, y, z, w = Rotation.from_euler("z", heading_rad).as_quat()
    return [float(w), float(x), float(y), float(z)]


def _size_to_wlh(sx, sy, sz):
    """Waymo (x=length, y=width, z=height) → ADDS [width, length, height]"""
    return [float(sy), float(sx), float(sz)]


def _micros_to_date(ts_micros: int) -> str:
    return dt.datetime.fromtimestamp(ts_micros / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d")


def _get_segment_ids():
    """vehicle_pose/*.parquet 파일 목록에서 세그먼트 ID 추출 (DAG 파싱 시 실행)"""
    paths = sorted(glob.glob(f"{WAYMO_BASE}/vehicle_pose/*.parquet"))
    return [os.path.basename(p).replace(".parquet", "") for p in paths]


# ─────────────────────────────────────────────────────────────
# 태스크 1: setup_global
# ─────────────────────────────────────────────────────────────
def setup_global(**kwargs):
    """
    전체 공통 데이터 1회 삽입 (get_or_create):
    - category : adds DB 우선 조회 → waymo DB 조회 → 없으면 신규
    - sensor   : waymo DB 조회 → 없으면 신규
    - dataset  : adds DB 우선 조회 → waymo DB 조회 → 없으면 신규
    """
    conn = _get_conn()
    cur  = conn.cursor()

    # ── category ──
    cur.execute(f"USE {ADDS_DB}")
    cur.execute("SELECT name, token FROM category")
    adds_category = dict(cur.fetchall())

    cur.execute(f"USE {STAGING_DB}")
    cur.execute("SELECT name, token FROM category")
    waymo_category = dict(cur.fetchall())

    cat_rows = []
    for type_int, cat_name in WAYMO_TYPE_TO_CATEGORY.items():
        if cat_name in adds_category:
            token = adds_category[cat_name]
            print(f"  [category] adds 기존: '{cat_name}'")
        elif cat_name in waymo_category:
            token = waymo_category[cat_name]
            print(f"  [category] waymo 기존: '{cat_name}'")
        else:
            token = secrets.token_hex(16)
            print(f"  [category] 신규: '{cat_name}'")
        cat_rows.append((token, cat_name, None))

    cur.executemany(
        "INSERT IGNORE INTO category (token, name, description) VALUES (%s, %s, %s)",
        cat_rows,
    )

    # ── sensor ──
    cur.execute("SELECT channel, token FROM sensor")
    existing_sensors = dict(cur.fetchall())

    sensor_rows = []
    for _, channel in {**CAMERA_CHANNEL_MAP, **{k: v for k, v in LIDAR_CHANNEL_MAP.items()}}.items():
        pass  # 아래에서 직접 처리
    sensor_rows = []
    for modality, channel_map in [("camera", CAMERA_CHANNEL_MAP), ("lidar", LIDAR_CHANNEL_MAP)]:
        for _, channel in channel_map.items():
            token = existing_sensors.get(channel) or secrets.token_hex(16)
            sensor_rows.append((token, channel, modality))

    cur.executemany(
        "INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s, %s, %s)",
        sensor_rows,
    )

    # ── dataset ──
    cur.execute(f"USE {ADDS_DB}")
    cur.execute("SELECT token FROM dataset WHERE name = 'Waymo Open Dataset'")
    row = cur.fetchone()
    if row:
        dataset_token = row[0]
        print(f"  [dataset] adds 기존: {dataset_token[:8]}...")
    else:
        cur.execute(f"USE {STAGING_DB}")
        cur.execute("SELECT token FROM dataset WHERE name = 'Waymo Open Dataset'")
        row = cur.fetchone()
        if row:
            dataset_token = row[0]
            print(f"  [dataset] waymo 기존: {dataset_token[:8]}...")
        else:
            dataset_token = secrets.token_hex(16)
            print(f"  [dataset] 신규 생성")

    cur.execute(f"USE {STAGING_DB}")
    cur.execute(
        "INSERT IGNORE INTO dataset (token, name) VALUES (%s, %s)",
        (dataset_token, "Waymo Open Dataset"),
    )

    conn.commit()
    conn.close()
    print(f"[완료] setup_global: category {len(cat_rows)}개 / sensor {len(sensor_rows)}개 / dataset 1개")


# ─────────────────────────────────────────────────────────────
# 태스크 2: process_segment
# ─────────────────────────────────────────────────────────────
def process_segment(segment_id: str, **kwargs):
    """
    세그먼트 1개에 대한 전체 삽입 순서:
      log → ego_pose → scene(NULL) → frame → scene UPDATE
      → calibrated_sensor → frame_data
      → scenario + scenarios_in_scene
      → instance → frame_annotation_new

    모든 token은 segment_id 기반 MD5 (결정론적) → 재실행 안전
    이미 처리된 세그먼트는 log token 존재 여부로 스킵
    """
    conn = _get_conn()
    cur  = conn.cursor()
    cur.execute(f"USE {STAGING_DB}")

    # ── 이미 처리된 세그먼트 스킵 ──
    log_token = _md5(f"log:{segment_id}")
    cur.execute("SELECT token FROM log WHERE token = %s", (log_token,))
    if cur.fetchone():
        print(f"[SKIP] {segment_id} 이미 처리됨")
        conn.close()
        return

    # ── 공통 token DB 조회 (XCom 대신 DB에서 직접 읽기) ──
    cur.execute("SELECT name, token FROM category")
    cat_name_to_token = dict(cur.fetchall())
    category_token_map = {
        type_int: cat_name_to_token.get(cat_name)
        for type_int, cat_name in WAYMO_TYPE_TO_CATEGORY.items()
    }

    cur.execute("SELECT channel, token FROM sensor")
    sensor_token_map = dict(cur.fetchall())

    cur.execute("SELECT token FROM dataset WHERE name = 'Waymo Open Dataset'")
    row = cur.fetchone()
    dataset_token = row[0] if row else _md5("dataset:Waymo Open Dataset")

    # ── 파일 경로 ──
    stats_path       = f"{WAYMO_BASE}/stats/{segment_id}.parquet"
    vp_path          = f"{WAYMO_BASE}/vehicle_pose/{segment_id}.parquet"
    cam_calib_path   = f"{WAYMO_BASE}/camera_calibration/{segment_id}.parquet"
    lidar_calib_path = f"{WAYMO_BASE}/lidar_calibration/{segment_id}.parquet"
    lidar_box_path   = f"{WAYMO_BASE}/lidar_box/{segment_id}.parquet"

    # ── stats ──
    stats_df    = pd.read_parquet(stats_path)
    first_stats = stats_df.iloc[0]
    time_of_day = str(first_stats["[StatsComponent].time_of_day"])
    weather     = str(first_stats["[StatsComponent].weather"])
    location    = str(first_stats["[StatsComponent].location"])

    # ── vehicle_pose ──
    vp_df      = pd.read_parquet(vp_path).sort_values("key.frame_timestamp_micros").reset_index(drop=True)
    timestamps = [int(ts) for ts in vp_df["key.frame_timestamp_micros"].tolist()]
    transforms = vp_df["[VehiclePoseComponent].world_from_vehicle.transform"].tolist()
    date_captured = _micros_to_date(timestamps[0])

    # ── log ──
    cur.execute(
        """INSERT IGNORE INTO log
           (token, dataset_token, device, date_captured, location, logfile, driving_distance)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (log_token, dataset_token, None, date_captured, location, None, None),
    )

    # ── ego_pose ──
    ego_pose_token_map = {}
    ego_pose_rows = []
    for ts, flat_t in zip(timestamps, transforms):
        token = _md5(f"ego_pose:{segment_id}:{ts}")
        ego_pose_token_map[ts] = token
        trans, rot = _parse_transform(flat_t)
        ego_pose_rows.append((
            token, ts,
            json.dumps(trans), json.dumps(rot),
            None,       # coordinate_system (Waymo world frame EPSG 미확정)
            "vehicle",
            None,       # velocity
            None,       # acceleration
            None,       # angular_rate
        ))

    cur.executemany(
        """INSERT IGNORE INTO ego_pose
           (token, timestamp, translation, rotation, coordinate_system,
            pose_type, velocity, acceleration, angular_rate)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        ego_pose_rows,
    )

    # ── scene (first/last_frame_token = NULL → frame 삽입 후 UPDATE) ──
    scene_token = _md5(f"scene:{segment_id}")
    base_path   = f"waymo/validation/{segment_id}"
    cur.execute(
        """INSERT IGNORE INTO scene
           (token, log_token, base_path, num_frames, first_frame_token, last_frame_token)
           VALUES (%s, %s, %s, %s, NULL, NULL)""",
        (scene_token, log_token, base_path, len(timestamps)),
    )

    # ── frame ──
    frame_token_map = {}
    frame_tokens    = [_md5(f"frame:{segment_id}:{ts}") for ts in timestamps]
    frame_rows      = []
    for i, (ts, ft) in enumerate(zip(timestamps, frame_tokens)):
        frame_token_map[ts] = ft
        prev_ft = frame_tokens[i - 1] if i > 0 else None
        next_ft = frame_tokens[i + 1] if i < len(frame_tokens) - 1 else None
        frame_rows.append((ft, ts, scene_token, prev_ft, next_ft))

    cur.executemany(
        "INSERT IGNORE INTO frame (token, timestamp, scene_token, prev, next) VALUES (%s, %s, %s, %s, %s)",
        frame_rows,
    )

    # ── scene first/last_frame_token UPDATE ──
    cur.execute(
        "UPDATE scene SET first_frame_token = %s, last_frame_token = %s WHERE token = %s",
        (frame_tokens[0], frame_tokens[-1], scene_token),
    )

    conn.commit()
    print(f"[{segment_id[:16]}] log/ego_pose/scene/frame 완료 ({len(timestamps)}프레임)")

    # ── calibrated_sensor ──
    cam_calibrated_sensor_map   = {}  # {channel: (cs_token, width, height)}
    lidar_calibrated_sensor_map = {}  # {channel: cs_token}
    cs_rows = []

    cam_df = pd.read_parquet(cam_calib_path)
    for _, r in cam_df.iterrows():
        channel    = CAMERA_CHANNEL_MAP[int(r["key.camera_name"])]
        token      = _md5(f"cs:{segment_id}:{channel}")
        trans, rot = _parse_transform(r["[CameraCalibrationComponent].extrinsic.transform"])
        intrinsic  = [
            [float(r["[CameraCalibrationComponent].intrinsic.f_u"]), 0.0,
             float(r["[CameraCalibrationComponent].intrinsic.c_u"])],
            [0.0, float(r["[CameraCalibrationComponent].intrinsic.f_v"]),
             float(r["[CameraCalibrationComponent].intrinsic.c_v"])],
            [0.0, 0.0, 1.0],
        ]
        distortion = [
            float(r["[CameraCalibrationComponent].intrinsic.k1"]),
            float(r["[CameraCalibrationComponent].intrinsic.k2"]),
            float(r["[CameraCalibrationComponent].intrinsic.p1"]),
            float(r["[CameraCalibrationComponent].intrinsic.p2"]),
            float(r["[CameraCalibrationComponent].intrinsic.k3"]),
        ]
        width  = int(r["[CameraCalibrationComponent].width"])
        height = int(r["[CameraCalibrationComponent].height"])
        cam_calibrated_sensor_map[channel] = (token, width, height)
        cs_rows.append((
            token, sensor_token_map[channel],
            json.dumps(trans), json.dumps(rot),
            json.dumps(intrinsic), trans[2],
            json.dumps(distortion),
        ))

    lidar_df = pd.read_parquet(lidar_calib_path)
    for _, r in lidar_df.iterrows():
        channel    = LIDAR_CHANNEL_MAP[int(r["key.laser_name"])]
        token      = _md5(f"cs:{segment_id}:{channel}")
        trans, rot = _parse_transform(r["[LiDARCalibrationComponent].extrinsic.transform"])
        lidar_calibrated_sensor_map[channel] = token
        cs_rows.append((
            token, sensor_token_map[channel],
            json.dumps(trans), json.dumps(rot),
            None, trans[2], None,
        ))

    cur.executemany(
        """INSERT IGNORE INTO calibrated_sensor
           (token, sensor_token, translation, rotation,
            camera_intrinsic, height_from_ground, distortion)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        cs_rows,
    )
    conn.commit()
    print(f"[{segment_id[:16]}] calibrated_sensor {len(cs_rows)}개 완료")

    # ── frame_data ──
    fd_rows = []
    for channel, (cs_token, width, height) in cam_calibrated_sensor_map.items():
        fd_tokens = [_md5(f"fd:{segment_id}:{channel}:{ts}") for ts in timestamps]
        for i, ts in enumerate(timestamps):
            frame_token    = frame_token_map.get(ts)
            ego_pose_token = ego_pose_token_map.get(ts)
            if not frame_token or not ego_pose_token:
                print(f"  [경고] channel={channel} ts={ts} token 없음 → 스킵")
                continue
            prev_fd = fd_tokens[i - 1] if i > 0 else None
            next_fd = fd_tokens[i + 1] if i < len(fd_tokens) - 1 else None
            fd_rows.append((
                fd_tokens[i], frame_token, ego_pose_token, cs_token,
                ts,
                1,      # is_key_frame = True (camera_image parquet 손상으로 기본값 사용)
                height, width,
                f"{channel}/{ts}.jpg",
                "jpg",
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
    print(f"[{segment_id[:16]}] frame_data {len(fd_rows)}개 완료")

    # ── scenario ──
    description = f"{time_of_day}, {weather}"

    cur.execute(f"USE {ADDS_DB}")
    cur.execute("SELECT token FROM scenario WHERE description = %s", (description,))
    row = cur.fetchone()
    cur.execute(f"USE {STAGING_DB}")

    if row:
        scenario_token = row[0]
        print(f"  [scenario] adds 기존: '{description}'")
    else:
        cur.execute("SELECT token FROM scenario WHERE description = %s", (description,))
        row = cur.fetchone()
        scenario_token = row[0] if row else secrets.token_hex(16)
        print(f"  [scenario] waymo {'기존' if row else '신규'}: '{description}'")

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
    lb_df = (
        pd.read_parquet(lidar_box_path)
        .sort_values(["key.laser_object_id", "key.frame_timestamp_micros"])
        .reset_index(drop=True)
    )

    # fa_token 결정론적 생성
    lb_df["fa_token"] = [
        _md5(f"fa:{segment_id}:{oid}:{int(ts)}")
        for oid, ts in zip(lb_df["key.laser_object_id"], lb_df["key.frame_timestamp_micros"])
    ]
    lb_df["prev_fa"] = lb_df.groupby("key.laser_object_id")["fa_token"].shift(1)
    lb_df["next_fa"] = lb_df.groupby("key.laser_object_id")["fa_token"].shift(-1)

    instance_rows   = []
    skipped_obj_ids = set()
    for obj_id, grp in lb_df.groupby("key.laser_object_id"):
        grp_sorted     = grp.sort_values("key.frame_timestamp_micros")
        type_int       = int(grp_sorted.iloc[0]["[LiDARBoxComponent].type"])
        category_token = category_token_map.get(type_int)
        if not category_token:
            print(f"  [경고] type={type_int} category_token 없음 → instance + fa 스킵 (obj_id={str(obj_id)[:8]}...)")
            skipped_obj_ids.add(obj_id)
            continue
        instance_token = _md5(obj_id)
        instance_rows.append((
            instance_token, category_token,
            len(grp_sorted),
            grp_sorted.iloc[0]["fa_token"],
            grp_sorted.iloc[-1]["fa_token"],
        ))

    cur.executemany(
        """INSERT IGNORE INTO instance
           (token, category_token, num_annotations,
            first_frame_annotation_token, last_frame_annotation_token)
           VALUES (%s, %s, %s, %s, %s)""",
        instance_rows,
    )

    fa_rows = []
    for _, r in lb_df.iterrows():
        obj_id = r["key.laser_object_id"]
        if obj_id in skipped_obj_ids:
            continue
        ts_int      = int(r["key.frame_timestamp_micros"])
        frame_token = frame_token_map.get(ts_int)
        if not frame_token:
            print(f"  [경고] ts={ts_int} frame_token 없음 → fa 스킵")
            continue

        trans_center = [
            float(r["[LiDARBoxComponent].box.center.x"]),
            float(r["[LiDARBoxComponent].box.center.y"]),
            float(r["[LiDARBoxComponent].box.center.z"]),
        ]
        size_wlh = _size_to_wlh(
            r["[LiDARBoxComponent].box.size.x"],
            r["[LiDARBoxComponent].box.size.y"],
            r["[LiDARBoxComponent].box.size.z"],
        )
        rot_ori  = _heading_to_quat(float(r["[LiDARBoxComponent].box.heading"]))
        velocity = [
            float(r["[LiDARBoxComponent].speed.x"]),
            float(r["[LiDARBoxComponent].speed.y"]),
            float(r["[LiDARBoxComponent].speed.z"]),
        ]
        prev_fa = r["prev_fa"] if pd.notna(r["prev_fa"]) else None
        next_fa = r["next_fa"] if pd.notna(r["next_fa"]) else None

        fa_rows.append((
            r["fa_token"],
            frame_token,
            None,               # frame_data_token (TODO: pcd 수집 후 연결)
            _md5(obj_id),
            "bbox_pcd3d",
            json.dumps(trans_center),
            json.dumps(size_wlh),
            json.dumps(rot_ori),
            int(r["[LiDARBoxComponent].num_lidar_points_in_box"]),
            None,               # num_radar_pts (Waymo radar 없음)
            None,               # attribute_tokens
            json.dumps(velocity),
            1,                  # is_lidar_synced = True
            1,                  # frame_aligned = True
            None,               # visibility_token
            None,               # bbox_image3d
            None,               # bbox_image2d
            None,               # description
            prev_fa,
            next_fa,
        ))

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
        print(f"  [{segment_id[:16]}] frame_annotation_new 배치 {start}~{start + len(batch) - 1}")

    conn.commit()
    conn.close()
    print(f"[{segment_id[:16]}] instance {len(instance_rows)}개 / frame_annotation_new {len(fa_rows)}개 완료")


# ─────────────────────────────────────────────────────────────
# DAG 정의
# ─────────────────────────────────────────────────────────────
def copy_to_adds():
    """
    waymo 스테이징 DB → adds DB 복사
    INSERT IGNORE 사용: adds에 이미 있는 데이터(category, sensor 등 공유 마스터)는 건너뜀
    """
    conn = _get_conn()
    cur  = conn.cursor()

    tables = [
        "category",
        "sensor",
        "dataset",
        "log",
        "ego_pose",
        "scene",
        "frame",
        "frame_data",
        "calibrated_sensor",
        "scenario",
        "scenarios_in_scene",
        "instance",
        "frame_annotation_new",
    ]

    for table in tables:
        print(f"  copying {table} ...")
        cur.execute(f"INSERT IGNORE INTO adds.{table} SELECT * FROM waymo.{table}")
        conn.commit()
        print(f"  {table} 완료")

    cur.close()
    conn.close()
    print("[완료] copy_to_adds")


# ─────────────────────────────────────────────────────────────
# DAG 정의
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="waymo_insert",
    default_args=default_args,
    description="Waymo Open Dataset → waymo staging DB 전체 세그먼트 삽입 후 adds DB 복사",
    schedule=None,
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=5,   # 동시 처리 세그먼트 수 (서버 부하에 따라 조절)
    tags=["waymo"],
) as dag:

    t_setup = PythonOperator(
        task_id="setup_global",
        python_callable=setup_global,
    )

    # 실행 시점에 vehicle_pose/*.parquet 목록을 읽어 XCom으로 전달 (파싱 시 고정 X)
    @task
    def get_segment_ids():
        ids = _get_segment_ids()
        print(f"[get_segment_ids] 세그먼트 {len(ids)}개 발견")
        return [{"segment_id": sid} for sid in ids]

    segment_kwargs = get_segment_ids()

    t_segments = PythonOperator.partial(
        task_id="process_segment",
        python_callable=process_segment,
    ).expand(op_kwargs=segment_kwargs)

    t_copy = PythonOperator(
        task_id="copy_to_adds",
        python_callable=copy_to_adds,
    )

    # setup_global과 get_segment_ids 모두 완료 후 process_segment 실행
    [t_setup, segment_kwargs] >> t_segments

    # 모든 세그먼트 처리 완료 후 adds로 복사
    t_segments >> t_copy