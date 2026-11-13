"""
morai_insert.py

MORAI 사고 시나리오 데이터를 staging DB(morai)에 적재한 뒤 adds DB로 복사하는
Airflow DAG. lyft_level5_insert.py와 같은 staging → copy_to_adds 구조.

설계 문서: airflow/docs/morai_etl_mapping.md
기준 DAG:  lyft_level5_insert.py

── 원천 ──────────────────────────────────────────────────────────────────────
/xvdz/morai/사고 시나리오 데이터/  (zip을 /xvdz/morai 에서 풀면 생기는 폴더)
  calibration/sensor_extrinsics/*.parquet  카메라 외부 파라미터
  camera/<채널>/<채널>.chunk_000N/scenarioK.mp4 (+ .parquet 2D 박스: 대상외)
  labels/egomotion/sensor_extrinsics.chunk_000N.csv  자차 egomotion

── 실행 방법 ─────────────────────────────────────────────────────────────────
1회차: 그냥 Trigger  → 프레임 추출 + staging 적재 + staging 검증 (adds는 건드리지 않음)
2회차: Trigger DAG w/ config  {"run_copy": true}  → 위 과정 + adds 복사 + adds 검증

staging은 매 실행 시작 때 비우고 다시 채운다(토큰이 결정적이라 결과가 같다).
adds 복사는 INSERT IGNORE / NOT EXISTS 라 두 번 실행돼도 행이 중복되지 않는다.

── 필요 패키지 (airflow_venv) ───────────────────────────────────────────────
pyarrow (parquet 읽기), imageio-ffmpeg (ffmpeg 바이너리. 시스템 ffmpeg가 있으면 그것을 우선 사용)
"""

import csv
import glob
import hashlib
import json
import math
import os
import secrets
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.providers.mysql.hooks.mysql import MySqlHook

KST = timezone(timedelta(hours=9))

# ── 상수 ──────────────────────────────────────────────────────────────────────
SOURCE_DIR    = "/xvdz/morai/사고 시나리오 데이터"
FRAMES_DIR    = "/xvdz/morai/frames"          # 추출 jpg. file_name 은 이 폴더 기준 상대경로
STAGING_DB    = "morai"
ADDS_DB       = "adds"
MYSQL_CONN_ID = "aws_mysql"

DATASET_NAME    = "morai"
SCENE_BASE_PATH = f"{DATASET_NAME}/"          # Lyft 관례: 데이터셋명 + "/"
SCENARIO_DESC   = DATASET_NAME                # 더미 scenario 1건 (Lyft 관례)
POSE_TYPE       = "vehicle"
IMG_WIDTH, IMG_HEIGHT = 1920, 1080            # 원천 mp4 실측값 (70개 모두 동일)
FFMPEG_WORKERS  = 4

# adds.sensor.channel 은 varchar(17)이라 원천 채널명(최장 25자)이 들어가지 않는다.
# adds 관례(nuScenes CAM_FRONT 등)에 맞춘 표준명으로 바꿔 적재한다.
# 화각(fov)은 frame_data.file_name 경로의 원천 채널명으로 남는다.
CHANNEL_MAP = {
    "camera_cross_left_120fov":  "CAM_CROSS_LEFT",
    "camera_cross_right_120fov": "CAM_CROSS_RIGHT",
    "camera_front_tele_30fov":   "CAM_FRONT_TELE",
    "camera_front_wide_120fov":  "CAM_FRONT_WIDE",
    "camera_rear_left_70fov":    "CAM_REAR_LEFT",
    "camera_rear_right_70fov":   "CAM_REAR_RIGHT",
    "camera_rear_tele_30fov":    "CAM_REAR_TELE",
}

# staging에 만드는 테이블 (adds와 같은 이름·컬럼: CREATE TABLE ... LIKE adds.*)
STAGING_TABLES = [
    "dataset", "sensor", "calibrated_sensor", "log", "scene",
    "scenario", "scenarios_in_scene", "ego_pose", "frame", "frame_data",
]

# copy_to_adds 대상 (참조 순서). dataset / scenarios_in_scene 은 NOT EXISTS로 따로 복사
COPY_TABLES = [
    "sensor", "calibrated_sensor", "log", "scene", "scenario",
    "ego_pose", "frame", "frame_data",
]

# 각 테이블 INSERT 컬럼 (build_rows 의 튜플 순서와 같다)
COLUMNS = {
    "dataset":            ["token", "name"],
    "sensor":             ["token", "channel", "modality"],
    "calibrated_sensor":  ["token", "sensor_token", "translation", "rotation",
                           "camera_intrinsic", "height_from_ground", "distortion"],
    "log":                ["token", "dataset_token", "device", "date_captured", "location",
                           "driving_distance", "device_wlh", "logfile"],
    "scene":              ["token", "log_token", "base_path", "num_frames",
                           "first_frame_token", "last_frame_token", "goal_ego_pose_token"],
    "scenario":           ["token", "description"],
    "scenarios_in_scene": ["scene_token", "scenario_token"],
    "ego_pose":           ["token", "timestamp", "translation", "rotation", "coordinate_system",
                           "pose_type", "velocity", "acceleration", "angular_rate"],
    "frame":              ["token", "timestamp", "scene_token", "prev", "next"],
    "frame_data":         ["token", "frame_token", "ego_pose_token", "calibrated_sensor_token",
                           "timestamp", "is_key_frame", "height", "width", "file_name",
                           "file_format", "prev", "next"],
}


# ── 헬퍼 (DB·Airflow 없이 동작하는 순수 함수) ────────────────────────────────

def tok(*parts) -> str:
    """원천에 token이 없으므로 결정적 32자 token 생성. 재실행해도 같은 값."""
    return hashlib.md5(("morai:" + ":".join(map(str, parts))).encode()).hexdigest()


def euler_deg_to_quat(roll, pitch, yaw):
    """roll/pitch/yaw(deg) → 쿼터니언 [w,x,y,z]. R = Rz(yaw)·Ry(pitch)·Rx(roll)."""
    r, p, y = (math.radians(v) / 2 for v in (roll, pitch, yaw))
    cr, sr, cp, sp, cy, sy = math.cos(r), math.sin(r), math.cos(p), math.sin(p), math.cos(y), math.sin(y)
    q = [
        cr * cp * cy + sr * sp * sy,
        sr * cp * cy - cr * sp * sy,
        cr * sp * cy + sr * cp * sy,
        cr * cp * sy - sr * sp * cy,
    ]
    return [round(v, 8) for v in q]


def _f(v):
    return float(v) if v not in (None, "") else 0.0


def discover_scenarios(source_dir):
    """
    chunk_000N 별로 시나리오명, 채널별 mp4 경로를 찾는다.
    반환: [{"chunk": "chunk_0000", "scenario": "scenario1", "mp4": {채널: 경로}}, ...]
    """
    cam_root = os.path.join(source_dir, "camera")
    channels = sorted(d for d in os.listdir(cam_root) if os.path.isdir(os.path.join(cam_root, d)))
    chunks = sorted({p.rsplit(".", 1)[-1] for ch in channels
                     for p in os.listdir(os.path.join(cam_root, ch))})
    out = []
    for chunk in chunks:
        mp4 = {}
        for ch in channels:
            files = glob.glob(os.path.join(cam_root, ch, f"{ch}.{chunk}", "*.mp4"))
            if len(files) != 1:
                raise ValueError(f"{ch}.{chunk}: mp4가 1개가 아님 ({len(files)})")
            mp4[ch] = files[0]
        names = {os.path.basename(p)[:-4] for p in mp4.values()}
        if len(names) != 1:
            raise ValueError(f"{chunk}: 채널별 시나리오명이 다름 {names}")
        out.append({"chunk": chunk, "scenario": names.pop(), "mp4": mp4})
    return channels, out


def read_egomotion(path):
    """egomotion csv → 고유 timestamp 기준 중복 제거, 시간순 정렬된 행 목록."""
    seen, rows = set(), []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ts_us = int(r["time_stamp.seconds"]) * 1_000_000 + int(r["time_stamp.nanos"]) // 1000
            if ts_us in seen:
                continue
            seen.add(ts_us)
            r["_ts_us"] = ts_us
            rows.append(r)
    rows.sort(key=lambda r: r["_ts_us"])
    return rows


def read_extrinsics(source_dir):
    """calibration parquet 전부 → {(scenario, channel): row}"""
    import pyarrow.parquet as pq
    out = {}
    for p in sorted(glob.glob(os.path.join(source_dir, "calibration", "sensor_extrinsics", "*.parquet"))):
        for r in pq.read_table(p).to_pylist():
            out[(r["clip_id"], r["sensor_name"])] = r
    return out


def build_rows(source_dir, dataset_token, scenario_token):
    """원천 → 테이블별 INSERT 행. 반환: (rows dict, plan dict)"""
    channels, scenarios = discover_scenarios(source_dir)
    extr = read_extrinsics(source_dir)

    rows = {t: [] for t in COLUMNS}
    rows["dataset"].append((dataset_token, DATASET_NAME))
    rows["scenario"].append((scenario_token, SCENARIO_DESC))
    for ch in channels:
        if ch not in CHANNEL_MAP:
            raise ValueError(f"CHANNEL_MAP에 없는 채널: {ch}")
        rows["sensor"].append((tok("sensor", ch), CHANNEL_MAP[ch], "camera"))

    plan = []   # 프레임 추출 계획: (mp4, out_dir, 예상 프레임 수)
    for sc in scenarios:
        name = sc["scenario"]
        ego = read_egomotion(os.path.join(source_dir, "labels", "egomotion",
                                          f"sensor_extrinsics.{sc['chunk']}.csv"))
        n = len(ego)
        if n == 0:
            raise ValueError(f"{name}: egomotion 행 없음")
        devices = {r["id"] for r in ego}
        if len(devices) != 1:
            raise ValueError(f"{name}: 차량 id가 여러 개 {devices}")

        log_t, scene_t = tok("log", name), tok("scene", name)
        rows["log"].append((log_t, dataset_token, devices.pop(), None, None, None, None, name))
        rows["scene"].append((scene_t, log_t, SCENE_BASE_PATH, n,
                              tok("frame", name, 0), tok("frame", name, n - 1), None))
        rows["scenarios_in_scene"].append((scene_t, scenario_token))

        for ch in channels:
            e = extr.get((name, ch))
            if e is None:
                raise ValueError(f"{name}/{ch}: 외부 파라미터 없음")
            rows["calibrated_sensor"].append((
                tok("cs", name, ch), tok("sensor", ch),
                json.dumps([e["x"], e["y"], e["z"]]),
                json.dumps(euler_deg_to_quat(e["r"], e["p"], e["y.1"])),
                None, e["z"], None,
            ))
            plan.append((sc["mp4"][ch], os.path.join(FRAMES_DIR, name, ch), n))

        for i, r in enumerate(ego):
            ts = str(r["_ts_us"])
            rows["ego_pose"].append((
                tok("ego", name, i), ts,
                json.dumps([_f(r["location.x"]), _f(r["location.y"]), _f(r["location.z"])]),
                json.dumps(euler_deg_to_quat(_f(r["rotation.x"]), _f(r["rotation.y"]), _f(r["rotation.z"]))),
                None, POSE_TYPE,
                json.dumps([_f(r["local_velocity.x"]), _f(r["local_velocity.y"]), _f(r["local_velocity.z"])]),
                json.dumps([_f(r["local_acceleration.x"]), _f(r["local_acceleration.y"]), _f(r["local_acceleration.z"])]),
                json.dumps([_f(r["angular_velocity.x"]), _f(r["angular_velocity.y"]), _f(r["angular_velocity.z"])]),
            ))
            rows["frame"].append((
                tok("frame", name, i), ts, scene_t,
                tok("frame", name, i - 1) if i > 0 else None,
                tok("frame", name, i + 1) if i < n - 1 else None,
            ))
            for ch in channels:
                rows["frame_data"].append((
                    tok("fd", name, ch, i), tok("frame", name, i), tok("ego", name, i),
                    tok("cs", name, ch), ts, 1, IMG_HEIGHT, IMG_WIDTH,
                    f"{name}/{ch}/{i:06d}.jpg", "jpg",
                    tok("fd", name, ch, i - 1) if i > 0 else None,
                    tok("fd", name, ch, i + 1) if i < n - 1 else None,
                ))
    return rows, plan


def ffmpeg_exe():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    import imageio_ffmpeg  # pip install imageio-ffmpeg
    return imageio_ffmpeg.get_ffmpeg_exe()


def extract_one(exe, mp4, out_dir, expected):
    """mp4 → out_dir/000000.jpg ... 이미 예상 개수만큼 있으면 건너뜀."""
    os.makedirs(out_dir, exist_ok=True)
    existing = glob.glob(os.path.join(out_dir, "*.jpg"))
    if len(existing) == expected:
        return "skip"
    for p in existing:
        os.remove(p)
    pattern = os.path.join(out_dir, "%06d.jpg")
    # -nostdin + stdin=DEVNULL: 터미널에서 띄운 scheduler 아래에서 ffmpeg가 tty stdin을 읽으려다
    # SIGTTIN으로 정지(멈춤)하는 것을 막는다.
    base = [exe, "-nostdin", "-v", "error", "-y", "-i", mp4]
    tail = ["-start_number", "0", "-q:v", "2", pattern]
    run = dict(capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=600)
    res = subprocess.run(base + ["-fps_mode", "passthrough"] + tail, **run)
    if res.returncode != 0 and "fps_mode" in res.stderr:      # 구버전 ffmpeg
        res = subprocess.run(base + ["-vsync", "0"] + tail, **run)
    if res.returncode != 0:
        raise RuntimeError(f"ffmpeg 실패 {mp4}: {res.stderr[-500:]}")
    got = len(glob.glob(os.path.join(out_dir, "*.jpg")))
    if got != expected:
        raise RuntimeError(f"프레임 수 불일치 {mp4}: 추출 {got} / 예상 {expected}")
    return "done"


# ── DB 헬퍼 ───────────────────────────────────────────────────────────────────

def _get_conn():
    hook = MySqlHook.get_hook(conn_id=MYSQL_CONN_ID)
    return hook.get_conn()


def _one(cur, sql, args=None):
    cur.execute(sql, args)
    r = cur.fetchone()
    return r[0] if r else None


def _lookup_tokens(cur):
    """dataset / scenario 는 adds에 있으면 재사용, 없으면 신규 (Lyft 관례)."""
    d = _one(cur, f"SELECT token FROM {ADDS_DB}.dataset WHERE name=%s LIMIT 1", (DATASET_NAME,))
    s = _one(cur, f"SELECT token FROM {ADDS_DB}.scenario WHERE description=%s LIMIT 1", (SCENARIO_DESC,))
    return d or secrets.token_hex(16), s or secrets.token_hex(16)


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "eun",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="morai_insert",
    default_args=default_args,
    description="MORAI 사고 시나리오 → morai staging DB 삽입 후 (run_copy=true일 때) adds DB 복사",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule_interval=None,
    catchup=False,
    params={"run_copy": False},
    tags=["morai", "etl"],
) as dag:

    @task
    def preflight():
        """패키지·원천·ffmpeg 확인, staging DB/테이블 준비 후 비우기."""
        import pyarrow  # noqa: F401  (없으면 여기서 실패: pip install pyarrow)
        exe = ffmpeg_exe()
        print(f"[preflight] ffmpeg: {exe}")
        if not os.path.isdir(os.path.join(SOURCE_DIR, "camera")):
            raise FileNotFoundError(f"원천 폴더 확인 필요: {SOURCE_DIR}")
        channels, scenarios = discover_scenarios(SOURCE_DIR)
        print(f"[preflight] 채널 {len(channels)}개, 시나리오 {len(scenarios)}개")

        conn = _get_conn()
        cur = conn.cursor()
        exists = _one(cur, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s", (STAGING_DB,))
        if exists:
            # 같은 이름의 DB가 다른 용도로 쓰이고 있으면 비우지 않고 중단
            has_ds = _one(cur, "SELECT COUNT(*) FROM information_schema.TABLES "
                               "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='dataset'", (STAGING_DB,))
            if has_ds:
                others = _one(cur, f"SELECT COUNT(*) FROM {STAGING_DB}.dataset WHERE name<>%s", (DATASET_NAME,))
                if others:
                    raise RuntimeError(f"{STAGING_DB} DB에 다른 데이터셋 행이 있음. 확인 전까지 중단")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {STAGING_DB}")
        for t in STAGING_TABLES:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {STAGING_DB}.{t} LIKE {ADDS_DB}.{t}")
            cur.execute(f"TRUNCATE TABLE {STAGING_DB}.{t}")
        conn.commit()
        cur.close()
        conn.close()
        print(f"[preflight] staging {STAGING_DB} 준비 완료 ({len(STAGING_TABLES)}개 테이블 비움)")

    @task
    def extract_frames():
        """mp4 70개 → jpg 12,488장 (FRAMES_DIR/<시나리오>/<채널>/NNNNNN.jpg)."""
        exe = ffmpeg_exe()
        print(f"[extract_frames] ffmpeg: {exe}", flush=True)
        _, plan = build_rows(SOURCE_DIR, "x" * 32, "x" * 32)
        print(f"[extract_frames] 영상 {len(plan)}개 → {FRAMES_DIR}", flush=True)

        def _run(a):
            r = extract_one(exe, *a)
            print(f"[extract_frames] {r} {a[1]} ({a[2]}장)", flush=True)
            return r

        with ThreadPoolExecutor(FFMPEG_WORKERS) as ex:
            results = list(ex.map(_run, plan))
        print(f"[extract_frames] 추출 {results.count('done')}개 영상, 기존 유지 {results.count('skip')}개 영상, "
              f"총 {sum(p[2] for p in plan)}장")

    @task
    def load_staging():
        """원천 → staging 전 테이블 INSERT (컬럼 길이 사전 점검 포함)."""
        conn = _get_conn()
        cur = conn.cursor()
        dataset_token, scenario_token = _lookup_tokens(cur)
        rows, _ = build_rows(SOURCE_DIR, dataset_token, scenario_token)

        # 문자열 컬럼 길이 점검: 넘치면 절삭되거나 실패하므로 INSERT 전에 막는다
        cur.execute(
            "SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, DATA_TYPE "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s", (ADDS_DB,))
        limits = {(t, c): (m, dt) for t, c, m, dt in cur.fetchall()}
        problems = []
        for t, rs in rows.items():
            for ci, c in enumerate(COLUMNS[t]):
                if (t, c) not in limits:
                    problems.append(f"{t}.{c}: adds에 컬럼 없음")
                    continue
                m, dt = limits[(t, c)]
                if m is None or dt == "json":
                    continue
                longest = max((len(str(r[ci])) for r in rs if isinstance(r[ci], str)), default=0)
                if longest > m:
                    problems.append(f"{t}.{c}: 최대 {longest}자 > 컬럼 {m}자")
        if problems:
            raise ValueError("컬럼 점검 실패\n" + "\n".join(problems))

        for t in STAGING_TABLES:
            cols = COLUMNS[t]
            sql = (f"INSERT INTO {STAGING_DB}.{t} ({', '.join(cols)}) "
                   f"VALUES ({', '.join(['%s'] * len(cols))})")
            for i in range(0, len(rows[t]), 5000):
                cur.executemany(sql, rows[t][i:i + 5000])
            print(f"[load_staging] {t} {len(rows[t])}건")
        conn.commit()
        cur.close()
        conn.close()
        return {t: len(rows[t]) for t in STAGING_TABLES}

    @task
    def verify_staging(expected: dict):
        """건수·token 중복·참조 정합성·jpg 존재 확인. 하나라도 틀리면 실패."""
        conn = _get_conn()
        cur = conn.cursor()
        errors = []
        for t, n in expected.items():
            got = _one(cur, f"SELECT COUNT(*) FROM {STAGING_DB}.{t}")
            if got != n:
                errors.append(f"{t}: {got}건 (예상 {n})")
            if "token" in COLUMNS[t]:
                d = _one(cur, f"SELECT COUNT(*) - COUNT(DISTINCT token) FROM {STAGING_DB}.{t}")
                if d:
                    errors.append(f"{t}: token 중복 {d}건")
        S = STAGING_DB
        orphan_checks = {
            "log→dataset":          f"SELECT COUNT(*) FROM {S}.log l LEFT JOIN {S}.dataset d ON d.token=l.dataset_token WHERE d.token IS NULL",
            "scene→log":            f"SELECT COUNT(*) FROM {S}.scene s LEFT JOIN {S}.log l ON l.token=s.log_token WHERE l.token IS NULL",
            "scene.first_frame":    f"SELECT COUNT(*) FROM {S}.scene s LEFT JOIN {S}.frame f ON f.token=s.first_frame_token WHERE f.token IS NULL",
            "scene.last_frame":     f"SELECT COUNT(*) FROM {S}.scene s LEFT JOIN {S}.frame f ON f.token=s.last_frame_token WHERE f.token IS NULL",
            "frame→scene":          f"SELECT COUNT(*) FROM {S}.frame f LEFT JOIN {S}.scene s ON s.token=f.scene_token WHERE s.token IS NULL",
            "frame_data→frame":     f"SELECT COUNT(*) FROM {S}.frame_data x LEFT JOIN {S}.frame f ON f.token=x.frame_token WHERE f.token IS NULL",
            "frame_data→ego_pose":  f"SELECT COUNT(*) FROM {S}.frame_data x LEFT JOIN {S}.ego_pose e ON e.token=x.ego_pose_token WHERE e.token IS NULL",
            "frame_data→cs":        f"SELECT COUNT(*) FROM {S}.frame_data x LEFT JOIN {S}.calibrated_sensor c ON c.token=x.calibrated_sensor_token WHERE c.token IS NULL",
            "cs→sensor":            f"SELECT COUNT(*) FROM {S}.calibrated_sensor c LEFT JOIN {S}.sensor s ON s.token=c.sensor_token WHERE s.token IS NULL",
            "scenarios_in_scene":   f"SELECT COUNT(*) FROM {S}.scenarios_in_scene x LEFT JOIN {S}.scene s ON s.token=x.scene_token WHERE s.token IS NULL",
        }
        for name, sql in orphan_checks.items():
            n = _one(cur, sql)
            if n:
                errors.append(f"참조 끊김 {name}: {n}건")
        cur.execute(f"SELECT file_name FROM {S}.frame_data")
        missing = [r[0] for r in cur.fetchall() if not os.path.isfile(os.path.join(FRAMES_DIR, r[0]))]
        if missing:
            errors.append(f"jpg 없음 {len(missing)}건 (예: {missing[:3]})")
        cur.close()
        conn.close()
        if errors:
            raise ValueError("staging 검증 실패\n" + "\n".join(errors))
        print(f"[verify_staging] 통과: {expected}")

    @task
    def copy_to_adds():
        """run_copy=true 로 트리거한 경우에만 staging → adds 복사 후 adds 건수 확인."""
        if not get_current_context()["params"].get("run_copy"):
            raise AirflowSkipException("run_copy=false → adds 복사 생략 (staging 검증까지만 수행)")
        conn = _get_conn()
        cur = conn.cursor()
        # dataset: PK/UNIQUE 없음 → name 기준 NOT EXISTS
        cur.execute(
            f"INSERT INTO {ADDS_DB}.dataset (token, name) "
            f"SELECT s.token, s.name FROM {STAGING_DB}.dataset s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {ADDS_DB}.dataset a WHERE a.name = s.name)")
        print(f"[copy_to_adds] dataset {cur.rowcount}건")
        for t in COPY_TABLES:
            cols = ", ".join(COLUMNS[t])
            cur.execute(f"INSERT IGNORE INTO {ADDS_DB}.{t} ({cols}) SELECT {cols} FROM {STAGING_DB}.{t}")
            print(f"[copy_to_adds] {t} {cur.rowcount}건")
        # scenarios_in_scene: PK 유무와 무관하게 중복 방지
        cur.execute(
            f"INSERT INTO {ADDS_DB}.scenarios_in_scene (scene_token, scenario_token) "
            f"SELECT s.scene_token, s.scenario_token FROM {STAGING_DB}.scenarios_in_scene s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {ADDS_DB}.scenarios_in_scene a "
            f"  WHERE a.scene_token=s.scene_token AND a.scenario_token=s.scenario_token)")
        print(f"[copy_to_adds] scenarios_in_scene {cur.rowcount}건")
        conn.commit()

        # adds 확인: 2번 검증 스크립트와 같은 계층 조인 기준
        A = ADDS_DB
        n_ds = _one(cur, f"SELECT COUNT(*) FROM {A}.dataset WHERE name=%s", (DATASET_NAME,))
        counts = {
            "log":        _one(cur, f"SELECT COUNT(*) FROM {A}.log l JOIN {A}.dataset d ON d.token=l.dataset_token WHERE d.name=%s", (DATASET_NAME,)),
            "scene":      _one(cur, f"SELECT COUNT(*) FROM {A}.scene s JOIN {A}.log l ON l.token=s.log_token JOIN {A}.dataset d ON d.token=l.dataset_token WHERE d.name=%s", (DATASET_NAME,)),
            "frame":      _one(cur, f"SELECT COUNT(*) FROM {A}.frame f JOIN {A}.scene s ON s.token=f.scene_token JOIN {A}.log l ON l.token=s.log_token JOIN {A}.dataset d ON d.token=l.dataset_token WHERE d.name=%s", (DATASET_NAME,)),
            "frame_data": _one(cur, f"SELECT COUNT(*) FROM {A}.frame_data x JOIN {A}.frame f ON f.token=x.frame_token JOIN {A}.scene s ON s.token=f.scene_token JOIN {A}.log l ON l.token=s.log_token JOIN {A}.dataset d ON d.token=l.dataset_token WHERE d.name=%s", (DATASET_NAME,)),
        }
        staging = {t: _one(cur, f"SELECT COUNT(*) FROM {STAGING_DB}.{t}") for t in counts}
        cur.close()
        conn.close()
        print(f"[copy_to_adds] adds.dataset name='{DATASET_NAME}' {n_ds}건, 계층 건수 {counts}")
        if n_ds != 1 or counts != staging:
            raise ValueError(f"adds 확인 실패: dataset {n_ds}건, adds {counts} / staging {staging}")
        print("[완료] adds 적재 확인")

    pre_t = preflight()
    ext_t = extract_frames()
    load_t = load_staging()
    ver_t = verify_staging(load_t)
    copy_t = copy_to_adds()

    pre_t >> ext_t >> load_t
    ver_t >> copy_t
