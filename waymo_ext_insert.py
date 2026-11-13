"""
waymo_ext_insert.py — Waymo Open Dataset v2.0.0 → adds_ext 신규 적재 DAG

기준 코드: dags/waymo_insert.py (수정하지 않음). 필드 처리와 token 규칙(MD5)은 그대로 가져와
adds 와 같은 token 이 만들어지게 한다. 저장 위치만 adds_ext 이고, 아래 5개 필드를 새로 적재한다.

  [CameraImageComponent].pose.transform / velocity(linear, angular) / pose_timestamp
      → 카메라 사진마다 ego_pose 행 1개 (nuScenes 처럼 사진별 자세)
        카메라 frame_data.ego_pose_token 이 이 행을 가리킨다
  [LiDARComponent].range_image_return1 / return2
      → 라이다 frame_data 행 (프레임 × 라이다 5대)
        값은 변환 없이 .npz 파일로 저장: {FILE_ROOT}/{scene.base_path}/{채널}/{ts}.npz
        frame_data.file_name = "{채널}/{ts}.npz" (카메라 jpg 와 같은 경로 규칙)

적재하지 않는 것 (11/7 확정): [StatsComponent].lidar_object_counts (매핑표 X 유지)
그대로 두는 것: frame_annotation_new.frame_data_token (기존 코드 TODO, 매핑표 대상 아님)

원칙
  - adds 는 읽기만 한다. 쓰기는 adds_ext 대상 문장만 실행되고, 그 밖의 쓰기는 즉시 중단한다.
  - 세그먼트 1개 = DB 트랜잭션 1개. 중간에 실패하면 그 세그먼트는 통째로 롤백되고,
    이미 끝난 세그먼트(adds_ext.log 에 token 있음)는 다시 실행할 때 건너뛴다.
  - 원본 parquet 은 세그먼트마다 받아서 처리한 뒤 바로 지운다.
  - 필수 값이 없으면 건너뛰지 않고 그 세그먼트를 실패로 기록한다. 실행 끝에 실패 목록을 보여 주고 태스크를 실패시킨다.

실행 (웹 화면 없이)
  source ~/airflow_venv/bin/activate
  export CLOUDSDK_CONFIG=/xvdz/gcloud_youmee
  airflow dags test waymo_ext_insert -c '{"segments": ["<세그먼트ID>"]}'   # 시험 1개
  airflow dags test waymo_ext_insert -c '{"workers": 2}'                    # 전체 202개 (끝난 세그먼트는 건너뜀)
  ※ nuPlan 복사·연결과 동시에 돌리지 않는다 (11/7 시험 때 같은 테이블 동시 쓰기로 교착 발생)
"""

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import traceback
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.spatial.transform import Rotation

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.mysql.hooks.mysql import MySqlHook

# ─────────────────────────────────────────────────────────────
# 상수
# ─────────────────────────────────────────────────────────────
SRC = "adds"
DST = "adds_ext"
CONN_ID = "aws_mysql"
DATASET_NAME = "Waymo Open Dataset"

GCS_BASE = "gs://waymo_open_dataset_v_2_0_0/validation"
GCLOUD = "/home/ubuntu/google-cloud-sdk/bin/gcloud"
CLOUDSDK_CONFIG = "/xvdz/gcloud_youmee"
RAW_ROOT = "/xvdz/waymo_ext/raw"          # 세그먼트별 원본 parquet (처리 후 삭제)
FILE_ROOT = "/xvdz/waymo_ext/files"       # 라이다 .npz 보관 (S3 키 구조와 같은 상대 경로)
COMPONENTS = ["stats", "vehicle_pose", "camera_calibration", "lidar_calibration",
              "lidar_box", "camera_image", "lidar"]

CAMERA_CHANNEL_MAP = {1: "FRONT", 2: "FRONT_LEFT", 3: "FRONT_RIGHT", 4: "SIDE_LEFT", 5: "SIDE_RIGHT"}
LIDAR_CHANNEL_MAP = {1: "TOP", 2: "LIDAR_FRONT", 3: "LIDAR_SIDE_LEFT", 4: "LIDAR_SIDE_RIGHT", 5: "REAR"}
WAYMO_TYPE_TO_CATEGORY = {1: "vehicle.car", 2: "pedestrian", 3: "static_object.sign", 4: "vehicle.bicycle"}

CI = "[CameraImageComponent]"
LI = "[LiDARComponent]"
LIDAR_BATCH = 10   # 라이다 parquet 은 한 번에 10행씩 읽는다 (통째로 읽으면 메모리 부족)

default_args = {"owner": "waymo_ext_etl", "retries": 0}

# ─────────────────────────────────────────────────────────────
# 쓰기 방지 가드
# ─────────────────────────────────────────────────────────────
_READ_OK = re.compile(r"^\s*(SELECT|SHOW|SET SESSION)\b", re.I)
_WRITE_OK = re.compile(rf"^\s*(INSERT (IGNORE )?INTO {DST}\.\w+\s|UPDATE {DST}\.\w+\s)", re.I)


def _run(cur, sql, args=None, many=False):
    s = " ".join(sql.split())
    if not (_READ_OK.match(s) or _WRITE_OK.match(s)):
        raise RuntimeError(f"허용되지 않은 SQL (adds_ext 외 쓰기 차단): {s[:120]}")
    return cur.executemany(sql, args) if many else cur.execute(sql, args)


def _one(cur, sql, args=None):
    _run(cur, sql, args)
    r = cur.fetchone()
    return r[0] if r else None


def _get_conn():
    conn = MySqlHook.get_hook(conn_id=CONN_ID).get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    _run(cur, "SET SESSION innodb_lock_wait_timeout=600, wait_timeout=28800, net_read_timeout=3600, net_write_timeout=3600")
    cur.close()
    return conn


# ─────────────────────────────────────────────────────────────
# 변환 헬퍼 (waymo_insert.py 와 같음)
# ─────────────────────────────────────────────────────────────
def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def _parse_transform(flat_arr):
    """float64[16] 행렬 → (translation [tx,ty,tz], rotation [w,x,y,z])"""
    m = np.array(flat_arr, dtype=np.float64).reshape(4, 4)
    translation = m[:3, 3].tolist()
    x, y, z, w = Rotation.from_matrix(m[:3, :3]).as_quat()
    return translation, [w, float(x), float(y), float(z)]


def _heading_to_quat(heading_rad: float):
    x, y, z, w = Rotation.from_euler("z", heading_rad).as_quat()
    return [float(w), float(x), float(y), float(z)]


def _size_to_wlh(sx, sy, sz):
    return [float(sy), float(sx), float(sz)]


def _micros_to_date(ts_micros: int) -> str:
    return dt.datetime.fromtimestamp(ts_micros / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d")


def _to_micros(v) -> int:
    """pose_timestamp: 초 단위(float)면 마이크로초로, 이미 마이크로초면 그대로"""
    f = float(v)
    return int(round(f * 1_000_000)) if f < 1e11 else int(f)


def _write_npz(path, arrays: dict):
    """np.load 로 읽히는 .npz. 압축 수준 1 (속도 우선)"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
        for k, a in arrays.items():
            with z.open(f"{k}.npy", "w", force_zip64=True) as f:
                np.lib.format.write_array(f, np.ascontiguousarray(a))
    os.replace(tmp, path)


# ─────────────────────────────────────────────────────────────
# 원본 다운로드
# ─────────────────────────────────────────────────────────────
def _download(segment_id):
    seg_dir = os.path.join(RAW_ROOT, segment_id)
    os.makedirs(seg_dir, exist_ok=True)
    env = dict(os.environ, CLOUDSDK_CONFIG=CLOUDSDK_CONFIG)
    paths = {}
    for comp in COMPONENTS:
        dst = os.path.join(seg_dir, f"{comp}.parquet")
        r = subprocess.run([GCLOUD, "storage", "cp", "-q", f"{GCS_BASE}/{comp}/{segment_id}.parquet", dst],
                           env=env, capture_output=True, text=True)
        if r.returncode != 0 or not os.path.isfile(dst):
            raise RuntimeError(f"다운로드 실패 {comp}: {r.stderr.strip()[-300:]}")
        paths[comp] = dst
    return seg_dir, paths


# ─────────────────────────────────────────────────────────────
# 세그먼트 1개 처리
# ─────────────────────────────────────────────────────────────
RETRY_MAX = 3            # DB 교착(1213)·잠금 대기 초과(1205)일 때 세그먼트를 처음부터 다시 적재하는 최대 횟수
RETRY_CODES = ("(1213,", "(1205,")


def process_segment(segment_id: str):
    """교착은 MySQL 이 '트랜잭션을 다시 시도하라'고 돌려주는 오류라, 롤백된 세그먼트를 처음부터 다시 적재한다.
    RETRY_MAX 번 모두 실패하면 지금처럼 실패로 남긴다. 그 밖의 오류는 다시 시도하지 않는다."""
    for attempt in range(1, RETRY_MAX + 1):
        seg, status, msg, sec = _process_segment_once(segment_id)
        if status == "FAIL" and msg.startswith(RETRY_CODES) and attempt < RETRY_MAX:
            time.sleep(10 * attempt)
            continue
        if attempt > 1:
            msg = f"{msg} (교착 재시도 {attempt - 1}회)"
        return seg, status, msg, sec


def _process_segment_once(segment_id: str):
    t0 = time.time()
    conn = _get_conn()
    cur = conn.cursor()
    seg_dir = None
    try:
        log_token = _md5(f"log:{segment_id}")
        if _one(cur, f"SELECT COUNT(*) FROM {DST}.log WHERE token=%s", (log_token,)):
            return segment_id, "SKIP", "이미 적재됨", 0

        # adds 에서 공유 token 조회 (읽기만)
        dataset_token = _one(cur, f"SELECT token FROM {SRC}.dataset WHERE name=%s", (DATASET_NAME,))
        if not dataset_token:
            raise RuntimeError("adds.dataset 에 Waymo 없음")
        _run(cur, f"SELECT name, token FROM {SRC}.category WHERE name IN %s",
             (tuple(WAYMO_TYPE_TO_CATEGORY.values()),))
        cat_name_to_token = dict(cur.fetchall())
        category_token_map = {t: cat_name_to_token.get(n) for t, n in WAYMO_TYPE_TO_CATEGORY.items()}

        scene_token = _md5(f"scene:{segment_id}")
        # base_path 는 adds 에 저장된 값을 그대로 쓴다 (11/8: adds 는 202개 모두 끝에 '/' 가 붙어 있음.
        # 기존 적재 코드 규칙과 다르므로 코드로 만들지 않고 adds 값을 가져온다)
        base_path = _one(cur, f"SELECT base_path FROM {SRC}.scene WHERE token=%s", (scene_token,))
        if not base_path:
            raise RuntimeError("adds 에 이 세그먼트 scene 이 없음 (적재 범위 밖)")

        t_dl = time.time()
        seg_dir, P = _download(segment_id)
        t_dl = time.time() - t_dl

        # ── stats ──
        st = pd.read_parquet(P["stats"]).iloc[0]
        time_of_day = str(st["[StatsComponent].time_of_day"])
        weather = str(st["[StatsComponent].weather"])
        location = str(st["[StatsComponent].location"])

        # ── vehicle_pose ──
        vp = pd.read_parquet(P["vehicle_pose"]).sort_values("key.frame_timestamp_micros").reset_index(drop=True)
        timestamps = [int(t) for t in vp["key.frame_timestamp_micros"].tolist()]
        transforms = vp["[VehiclePoseComponent].world_from_vehicle.transform"].tolist()
        ts_set = set(timestamps)

        # ── log ──
        _run(cur, f"""INSERT IGNORE INTO {DST}.log
                 (token, dataset_token, device, date_captured, location, logfile, driving_distance)
                 VALUES (%s,%s,%s,%s,%s,%s,%s)""",
             (log_token, dataset_token, None, _micros_to_date(timestamps[0]), location, None, None))

        # ── ego_pose (프레임 단위 차량 자세: 기존과 같음) ──
        ego_pose_token_map, rows = {}, []
        for ts, flat in zip(timestamps, transforms):
            tok = _md5(f"ego_pose:{segment_id}:{ts}")
            ego_pose_token_map[ts] = tok
            tr, ro = _parse_transform(flat)
            rows.append((tok, ts, json.dumps(tr), json.dumps(ro), None, "vehicle", None, None, None))

        # ── ego_pose (카메라 사진별 자세: 신규) ──
        cam_cols = ["key.frame_timestamp_micros", "key.camera_name", f"{CI}.pose.transform",
                    f"{CI}.velocity.linear_velocity.x", f"{CI}.velocity.linear_velocity.y",
                    f"{CI}.velocity.linear_velocity.z", f"{CI}.velocity.angular_velocity.x",
                    f"{CI}.velocity.angular_velocity.y", f"{CI}.velocity.angular_velocity.z",
                    f"{CI}.pose_timestamp"]
        ci = pq.read_table(P["camera_image"], columns=cam_cols).to_pandas()
        for c in cam_cols:
            if ci[c].isna().any():
                raise RuntimeError(f"camera_image {c} 에 NULL 있음")
        cam_pose_token = {}
        for r in ci.itertuples(index=False):
            ts, cam = int(r[0]), int(r[1])
            if ts not in ts_set:
                raise RuntimeError(f"camera_image ts={ts} 가 vehicle_pose 에 없음")
            ch = CAMERA_CHANNEL_MAP[cam]
            tok = _md5(f"ego_pose_cam:{segment_id}:{ch}:{ts}")
            cam_pose_token[(ch, ts)] = tok
            tr, ro = _parse_transform(r[2])
            rows.append((tok, _to_micros(r[9]), json.dumps(tr), json.dumps(ro), None, "vehicle",
                         json.dumps([float(r[3]), float(r[4]), float(r[5])]), None,
                         json.dumps([float(r[6]), float(r[7]), float(r[8])])))
        expected = len(timestamps) * len(CAMERA_CHANNEL_MAP)
        if len(cam_pose_token) != expected:
            raise RuntimeError(f"camera_image 사진 {len(cam_pose_token)}건 ≠ 프레임×카메라 {expected}건")
        del ci

        _run(cur, f"""INSERT IGNORE INTO {DST}.ego_pose
                 (token, timestamp, translation, rotation, coordinate_system,
                  pose_type, velocity, acceleration, angular_rate)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows, many=True)

        # ── scene / frame (기존과 같음) ──
        frame_tokens = [_md5(f"frame:{segment_id}:{ts}") for ts in timestamps]
        frame_token_map = dict(zip(timestamps, frame_tokens))
        _run(cur, f"""INSERT IGNORE INTO {DST}.scene
                 (token, log_token, base_path, num_frames, first_frame_token, last_frame_token)
                 VALUES (%s,%s,%s,%s,%s,%s)""",
             (scene_token, log_token, base_path, len(timestamps), frame_tokens[0], frame_tokens[-1]))
        _run(cur, f"INSERT IGNORE INTO {DST}.frame (token, timestamp, scene_token, prev, next) VALUES (%s,%s,%s,%s,%s)",
             [(ft, ts, scene_token, frame_tokens[i - 1] if i else None,
               frame_tokens[i + 1] if i < len(frame_tokens) - 1 else None)
              for i, (ts, ft) in enumerate(zip(timestamps, frame_tokens))], many=True)

        # ── calibrated_sensor (기존과 같음, sensor token 은 adds 의 같은 행에서) ──
        cam_cs, lidar_cs, cs_rows = {}, {}, []
        for _, r in pd.read_parquet(P["camera_calibration"]).iterrows():
            ch = CAMERA_CHANNEL_MAP[int(r["key.camera_name"])]
            tok = _md5(f"cs:{segment_id}:{ch}")
            tr, ro = _parse_transform(r["[CameraCalibrationComponent].extrinsic.transform"])
            K = [[float(r["[CameraCalibrationComponent].intrinsic.f_u"]), 0.0, float(r["[CameraCalibrationComponent].intrinsic.c_u"])],
                 [0.0, float(r["[CameraCalibrationComponent].intrinsic.f_v"]), float(r["[CameraCalibrationComponent].intrinsic.c_v"])],
                 [0.0, 0.0, 1.0]]
            D = [float(r[f"[CameraCalibrationComponent].intrinsic.{k}"]) for k in ("k1", "k2", "p1", "p2", "k3")]
            cam_cs[ch] = (tok, int(r["[CameraCalibrationComponent].width"]), int(r["[CameraCalibrationComponent].height"]))
            cs_rows.append([tok, None, json.dumps(tr), json.dumps(ro), json.dumps(K), tr[2], json.dumps(D)])
        for _, r in pd.read_parquet(P["lidar_calibration"]).iterrows():
            ch = LIDAR_CHANNEL_MAP[int(r["key.laser_name"])]
            tok = _md5(f"cs:{segment_id}:{ch}")
            tr, ro = _parse_transform(r["[LiDARCalibrationComponent].extrinsic.transform"])
            lidar_cs[ch] = tok
            cs_rows.append([tok, None, json.dumps(tr), json.dumps(ro), None, tr[2], None])
        _run(cur, f"SELECT token, sensor_token FROM {SRC}.calibrated_sensor WHERE token IN %s",
             (tuple(r[0] for r in cs_rows),))
        sensor_of = dict(cur.fetchall())
        for r in cs_rows:
            if r[0] not in sensor_of:
                raise RuntimeError(f"adds.calibrated_sensor 에 {r[0]} 없음")
            r[1] = sensor_of[r[0]]
        _run(cur, f"""INSERT IGNORE INTO {DST}.calibrated_sensor
                 (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground, distortion)
                 VALUES (%s,%s,%s,%s,%s,%s,%s)""", [tuple(r) for r in cs_rows], many=True)

        # ── frame_data 카메라 (token·경로는 기존과 같음, ego_pose 는 사진별 자세) ──
        fd_rows = []
        for ch, (cs_tok, width, height) in cam_cs.items():
            toks = [_md5(f"fd:{segment_id}:{ch}:{ts}") for ts in timestamps]
            for i, ts in enumerate(timestamps):
                fd_rows.append((toks[i], frame_token_map[ts], cam_pose_token[(ch, ts)], cs_tok, ts, 1,
                                height, width, f"{ch}/{ts}.jpg", "jpg",
                                toks[i - 1] if i else None, toks[i + 1] if i < len(toks) - 1 else None))

        # ── frame_data 라이다 (신규) + .npz 파일 ──
        lidar_keys = []
        pf = pq.ParquetFile(P["lidar"])
        cols = ["key.frame_timestamp_micros", "key.laser_name",
                f"{LI}.range_image_return1.values", f"{LI}.range_image_return1.shape",
                f"{LI}.range_image_return2.values", f"{LI}.range_image_return2.shape"]
        for b in pf.iter_batches(batch_size=LIDAR_BATCH, columns=cols):
            tss, lns = b.column(0).to_pylist(), b.column(1).to_pylist()
            for i in range(b.num_rows):
                ts, ch = int(tss[i]), LIDAR_CHANNEL_MAP[int(lns[i])]
                if ts not in ts_set:
                    raise RuntimeError(f"lidar ts={ts} 가 vehicle_pose 에 없음")
                arrs = {}
                for k, (vi, si) in {"range_image_return1": (2, 3), "range_image_return2": (4, 5)}.items():
                    shape = b.column(si)[i].as_py()
                    vals = b.column(vi)[i].values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
                    arrs[k] = vals.reshape(shape)
                _write_npz(os.path.join(FILE_ROOT, base_path, ch, f"{ts}.npz"), arrs)
                lidar_keys.append((ch, ts))
        expected = len(timestamps) * len(LIDAR_CHANNEL_MAP)
        if len(set(lidar_keys)) != expected:
            raise RuntimeError(f"lidar {len(set(lidar_keys))}건 ≠ 프레임×라이다 {expected}건")
        for ch, cs_tok in lidar_cs.items():
            toks = [_md5(f"fd_lidar:{segment_id}:{ch}:{ts}") for ts in timestamps]
            for i, ts in enumerate(timestamps):
                fd_rows.append((toks[i], frame_token_map[ts], ego_pose_token_map[ts], cs_tok, ts, 1,
                                None, None, f"{ch}/{ts}.npz", "npz",
                                toks[i - 1] if i else None, toks[i + 1] if i < len(toks) - 1 else None))
        _run(cur, f"""INSERT IGNORE INTO {DST}.frame_data
                 (token, frame_token, ego_pose_token, calibrated_sensor_token, timestamp, is_key_frame,
                  height, width, file_name, file_format, prev, next)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fd_rows, many=True)

        # ── scenarios_in_scene (공유 행 dataset·category·sensor·scenario 는 run_all 에서 미리 복사) ──
        description = f"{time_of_day}, {weather}"
        scenario_token = _one(cur, f"SELECT token FROM {SRC}.scenario WHERE description=%s", (description,))
        if not scenario_token or not _one(cur, f"SELECT COUNT(*) FROM {DST}.scenario WHERE token=%s", (scenario_token,)):
            raise RuntimeError(f"scenario '{description}' 가 adds 또는 adds_ext 에 없음")
        _run(cur, f"INSERT IGNORE INTO {DST}.scenarios_in_scene (scene_token, scenario_token) VALUES (%s,%s)",
             (scene_token, scenario_token))

        # ── instance + frame_annotation_new (기존과 같음) ──
        lb = (pd.read_parquet(P["lidar_box"])
              .sort_values(["key.laser_object_id", "key.frame_timestamp_micros"]).reset_index(drop=True))
        lb["fa_token"] = [_md5(f"fa:{segment_id}:{o}:{int(t)}")
                          for o, t in zip(lb["key.laser_object_id"], lb["key.frame_timestamp_micros"])]
        lb["prev_fa"] = lb.groupby("key.laser_object_id")["fa_token"].shift(1)
        lb["next_fa"] = lb.groupby("key.laser_object_id")["fa_token"].shift(-1)
        inst_rows, skipped = [], set()
        for oid, g in lb.groupby("key.laser_object_id"):
            g = g.sort_values("key.frame_timestamp_micros")
            ct = category_token_map.get(int(g.iloc[0]["[LiDARBoxComponent].type"]))
            if not ct:          # 기존 코드와 같은 처리 (adds 와 같은 결과를 내기 위함)
                skipped.add(oid)
                continue
            inst_rows.append((_md5(oid), ct, len(g), g.iloc[0]["fa_token"], g.iloc[-1]["fa_token"]))
        _run(cur, f"""INSERT IGNORE INTO {DST}.instance
                 (token, category_token, num_annotations, first_frame_annotation_token, last_frame_annotation_token)
                 VALUES (%s,%s,%s,%s,%s)""", inst_rows, many=True)
        fa_rows = []
        for _, r in lb.iterrows():
            if r["key.laser_object_id"] in skipped:
                continue
            ts = int(r["key.frame_timestamp_micros"])
            if ts not in frame_token_map:
                raise RuntimeError(f"lidar_box ts={ts} 가 vehicle_pose 에 없음")
            fa_rows.append((
                r["fa_token"], frame_token_map[ts], None, _md5(r["key.laser_object_id"]), "bbox_pcd3d",
                json.dumps([float(r[f"[LiDARBoxComponent].box.center.{a}"]) for a in "xyz"]),
                json.dumps(_size_to_wlh(r["[LiDARBoxComponent].box.size.x"], r["[LiDARBoxComponent].box.size.y"],
                                        r["[LiDARBoxComponent].box.size.z"])),
                json.dumps(_heading_to_quat(float(r["[LiDARBoxComponent].box.heading"]))),
                int(r["[LiDARBoxComponent].num_lidar_points_in_box"]), None, None,
                json.dumps([float(r[f"[LiDARBoxComponent].speed.{a}"]) for a in "xyz"]),
                1, 1, None, None, None, None,
                r["prev_fa"] if pd.notna(r["prev_fa"]) else None,
                r["next_fa"] if pd.notna(r["next_fa"]) else None))
        for s in range(0, len(fa_rows), 50_000):
            _run(cur, f"""INSERT IGNORE INTO {DST}.frame_annotation_new
                     (token, frame_token, frame_data_token, instance_token, annotation_type_name,
                      translation_center, size_wlh, rotation_orientation, num_lidar_pts, num_radar_pts,
                      attribute_tokens, velocity, is_lidar_synced, frame_aligned, visibility_token,
                      bbox_image3d, bbox_image2d, description, prev, next)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                 fa_rows[s:s + 50_000], many=True)

        conn.commit()
        msg = (f"frame {len(timestamps)}, 카메라 fd {len(timestamps) * 5}, 라이다 fd {len(timestamps) * 5}, "
               f"사진별 ego_pose {len(cam_pose_token)}, 박스 {len(fa_rows)} (다운로드 {t_dl:.0f}s)")
        return segment_id, "OK", msg, time.time() - t0
    except Exception as e:
        conn.rollback()
        return segment_id, "FAIL", f"{e}\n{traceback.format_exc()[-800:]}", time.time() - t0
    finally:
        conn.close()
        if seg_dir and os.path.isdir(seg_dir):
            shutil.rmtree(seg_dir, ignore_errors=True)


# ─────────────────────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────────────────────
def run_all(**_):
    conf = (get_current_context()["dag_run"].conf or {})
    workers = int(conf.get("workers", 2))
    conn = _get_conn()
    cur = conn.cursor()
    if not _one(cur, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s", (DST,)):
        raise RuntimeError(f"{DST} 없음 (nuPlan 단계에서 생성)")
    _run(cur, f"""SELECT s.base_path FROM {SRC}.scene s JOIN {SRC}.log l ON l.token=s.log_token
              JOIN {SRC}.dataset d ON d.token=l.dataset_token WHERE d.name=%s ORDER BY s.base_path""",
         (DATASET_NAME,))
    all_segs = [os.path.basename(r[0].rstrip("/")) for r in cur.fetchall()]
    segs = conf.get("segments") or all_segs
    unknown = sorted(set(segs) - set(all_segs))
    if unknown:
        raise RuntimeError(f"adds 에 없는 세그먼트: {unknown}")

    # 여러 세그먼트가 같이 쓰는 행(dataset·category·sensor·scenario)은 처리 전에 한 번만 복사
    # (세그먼트 트랜잭션끼리 같은 행을 잡고 기다리지 않게 하기 위함)
    _run(cur, f"""INSERT INTO {DST}.dataset SELECT * FROM {SRC}.dataset WHERE name=%s
             AND NOT EXISTS (SELECT 1 FROM (SELECT name FROM {DST}.dataset) x WHERE x.name=%s)""",
         (DATASET_NAME, DATASET_NAME))
    # category 는 nuPlan 과 같은 token 을 쓸 수 있어, nuPlan 복사(category 단계)가 끝나기 전에는
    # {"skip_category": true} 로 건너뛴다. 전체 실행 때 복사한다.
    if not conf.get("skip_category"):
        _run(cur, f"INSERT IGNORE INTO {DST}.category SELECT * FROM {SRC}.category WHERE name IN %s",
             (tuple(WAYMO_TYPE_TO_CATEGORY.values()),))
    else:
        print("[알림] category 복사 건너뜀 (skip_category)", flush=True)
    cs_tokens = tuple(_md5(f"cs:{s}:{ch}") for s in all_segs
                      for ch in list(CAMERA_CHANNEL_MAP.values()) + list(LIDAR_CHANNEL_MAP.values()))
    _run(cur, f"""INSERT IGNORE INTO {DST}.sensor SELECT * FROM {SRC}.sensor WHERE token IN
             (SELECT DISTINCT sensor_token FROM {SRC}.calibrated_sensor WHERE token IN %s)""", (cs_tokens,))
    _run(cur, f"""INSERT IGNORE INTO {DST}.scenario SELECT sc.* FROM {SRC}.scenario sc WHERE sc.token IN
             (SELECT x.scenario_token FROM {SRC}.scenarios_in_scene x JOIN {SRC}.scene s ON s.token=x.scene_token
              JOIN {SRC}.log l ON l.token=s.log_token JOIN {SRC}.dataset d ON d.token=l.dataset_token
              WHERE d.name=%s)""", (DATASET_NAME,))
    conn.commit()
    if not os.path.isfile(GCLOUD) or not os.path.isdir(CLOUDSDK_CONFIG):
        raise RuntimeError("gcloud 또는 로그인 설정 폴더 없음")
    os.makedirs(RAW_ROOT, exist_ok=True)
    os.makedirs(FILE_ROOT, exist_ok=True)
    conn.close()

    print(f"[시작] 세그먼트 {len(segs)}개 (adds 기준 {len(all_segs)}개), workers={workers}", flush=True)
    t0, done, results = time.time(), 0, []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(process_segment, s) for s in segs]
        for f in as_completed(futs):
            seg, status, msg, sec = f.result()
            done += 1
            results.append((seg, status, msg))
            el = time.time() - t0
            eta = el / done * (len(segs) - done)
            print(f"[{done}/{len(segs)}] {status} {seg} {sec:.0f}s | {msg.splitlines()[0]} "
                  f"| 경과 {el/60:.0f}분 남음 약 {eta/60:.0f}분", flush=True)
            if status == "FAIL":
                print(msg, flush=True)
    fails = [r for r in results if r[1] == "FAIL"]
    print(f"[끝] OK {sum(r[1] == 'OK' for r in results)} / SKIP {sum(r[1] == 'SKIP' for r in results)} "
          f"/ FAIL {len(fails)} / 총 {(time.time() - t0)/60:.0f}분", flush=True)
    if fails:
        raise RuntimeError("실패 세그먼트: " + ", ".join(r[0] for r in fails) + " (다시 실행하면 이어서 진행)")


with DAG(
    dag_id="waymo_ext_insert",
    default_args=default_args,
    description="Waymo v2.0.0 → adds_ext 신규 적재 (카메라 사진별 자세·라이다 포함)",
    schedule=None,
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    tags=["waymo", "adds_ext"],
) as dag:
    PythonOperator(task_id="run_all", python_callable=run_all)
