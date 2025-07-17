import datetime as dt
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.empty import EmptyOperator
import os
import sqlite3
import json
import secrets
import pickle
import io
import glob
from pathlib import Path
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 데이터 경로 설정
DATA_BASE_PATH = '/xvdz/nuplan-v1.1_mini/data/cache/mini'

# Pickle 안전하게 로드하기 위한 클래스들
class Dummy(list):
    def __new__(cls, *args, **kwargs):
        return list(args) if args else []

class SafeUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module.startswith("nuplan"):
            return Dummy
        return super().find_class(module, name)

def safe_pickle_loads(blob):
    if not blob:
        return None
    try:
        return SafeUnpickler(io.BytesIO(blob)).load()
    except Exception as e:
        print(f"pickle load error: {e}")
        return None

def blob_to_token(blob):
    return blob.hex() if blob else None

def convert_timestamp_to_datetime(timestamp_microseconds):
    timestamp_seconds = timestamp_microseconds / 1_000_000
    dt_object = datetime.fromtimestamp(timestamp_seconds)
    return dt_object.strftime("%Y-%m-%d %H:%M:%S")

class NuPlanDataProcessor:
    def __init__(self, db_file, dataset_token=None):
        self.db_file = db_file
        self.dataset_token = dataset_token
        self.sqlite_conn = None
        self.sqlite_cur = None
        self.mysql_hook = MySqlHook(mysql_conn_id='aws_mysql', schema='nuPlan')  # DB 이름 지정
        self.mysql_conn = None
        self.mysql_cur = None
        
    def __enter__(self):
        self.sqlite_conn = sqlite3.connect(self.db_file)
        self.sqlite_cur = self.sqlite_conn.cursor()
        self.mysql_conn = self.mysql_hook.get_conn()
        self.mysql_cur = self.mysql_conn.cursor()
        
        # DB 선택
        self.mysql_cur.execute("USE nuPlan")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sqlite_cur:
            self.sqlite_cur.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        if self.mysql_cur:
            self.mysql_cur.close()
        if self.mysql_conn:
            self.mysql_conn.close()
            
    def process_ego_pose(self):
        try:
            self.sqlite_cur.execute("""
                SELECT token, timestamp,
                       x, y, z,
                       qw, qx, qy, qz,
                       vx, vy, vz,
                       acceleration_x, acceleration_y, acceleration_z,
                       angular_rate_x, angular_rate_y, angular_rate_z,
                       epsg
                FROM ego_pose
            """)
            
            insert_query = """
            INSERT INTO ego_pose (
                token, timestamp,
                translation, rotation, velocity,
                acceleration, angular_rate,
                epsg, pose_type
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            batch_size = 10000
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    timestamp = str(row[1])
                    translation = json.dumps([row[2], row[3], row[4]])
                    rotation = json.dumps([row[5], row[6], row[7], row[8]])
                    velocity = json.dumps([row[9], row[10], row[11]])
                    acceleration = json.dumps([row[12], row[13], row[14]])
                    angular_rate = json.dumps([row[15], row[16], row[17]])
                    epsg = row[18]
                    pose_type = "vehicle"
                    
                    data.append((
                        token, timestamp,
                        translation, rotation, velocity,
                        acceleration, angular_rate,
                        epsg, pose_type
                    ))
                
                self.mysql_cur.executemany(insert_query, data)
                self.mysql_conn.commit()
                print(f"Inserted {len(data)} ego_pose rows from {self.db_file}")
                
        except Exception as e:
            print(f"Error processing ego_pose in {self.db_file}: {e}")
            raise

    def process_category(self):
        try:
            self.sqlite_cur.execute("""
                SELECT token, name, description
                FROM category
            """)
            rows = self.sqlite_cur.fetchall()
            
            data = []
            for row in rows:
                token = blob_to_token(row[0])
                name = row[1]
                description = row[2]
                data.append((token, name, description))
            
            self.mysql_cur.executemany("""
                INSERT IGNORE INTO category (
                    token, name, description
                ) VALUES (%s, %s, %s)
            """, data)
            self.mysql_conn.commit()
            print(f"Processed {len(data)} category rows from {self.db_file}")
            
        except Exception as e:
            print(f"Error processing category in {self.db_file}: {e}")
            raise

    def process_instance(self):
        try:
            total_start = time.time()

            # 1. track 테이블 행 수 확인
            count_start = time.time()
            self.sqlite_cur.execute("SELECT COUNT(*) FROM track")
            track_count = self.sqlite_cur.fetchone()[0]
            count_end = time.time()
            print(f"track 테이블 행 수 확인 시간: {count_end - count_start:.3f}초")
            print(f"  - track 테이블: {track_count:,}행")

            # 2. 조인 없이 track 테이블에서 필요한 컬럼만 가져오기
            query_start = time.time()
            print("track 테이블에서 데이터 조회 중...")
            self.sqlite_cur.execute("""
                SELECT token, category_token, width, length, height
                FROM track
            """)
            query_end = time.time()
            print(f"조회 쿼리 실행 시간: {query_end - query_start:.3f}초")

            insert_query = """
            INSERT INTO track (
                token,
                category_token,
                size
            ) VALUES (
                %s, %s, %s
            )
            """

            batch_size = 10000
            batch_count = 0

            while True:
                batch_start = time.time()

                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break

                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    category_token = blob_to_token(row[1])
                    size = json.dumps([row[2], row[3], row[4]])

                    data.append((
                        token,
                        category_token,
                        size
                    ))

                execute_start = time.time()
                self.mysql_cur.executemany(insert_query, data)
                execute_end = time.time()

                commit_start = time.time()
                self.mysql_conn.commit()
                commit_end = time.time()

                batch_total_time = commit_end - batch_start

                batch_count += 1

                print(f"배치 {batch_count} ({len(data)}행) 처리 시간:")
                print(f"  - Execute: {execute_end - execute_start:.3f}초")
                print(f"  - Commit: {commit_end - commit_start:.3f}초")
                print(f"  - 배치 총 시간: {batch_total_time:.3f}초")
                print(f"  - 초당 처리 레코드: {len(data)/batch_total_time:.1f} records/sec")
                print("-" * 50)

            total_end = time.time()
            total_time = total_end - total_start

            print(f"\n=== track 처리 완료 ({self.db_file}) ===")
            print(f"총 처리 시간: {total_time:.3f}초")
            print(f"총 배치 수: {batch_count}")
            print(f"처리된 총 레코드 수: {track_count}")

        except Exception as e:
            print(f"Error processing track in {self.db_file}: {e}")
            raise

    def process_frame_data(self):
        try:
            # 1. Image 데이터 처리
            self.sqlite_cur.execute("""
                SELECT token, next_token, prev_token, ego_pose_token, 
                    camera_token, timestamp, filename_jpg
                FROM image
            """)
            
            frame_insert_query = """
            INSERT INTO frame (
                token, scene_token
            ) VALUES (
                %s, %s
            )
            """
            
            frame_data_insert_query = """
            INSERT INTO frame_data (
                token, frame_token, ego_pose_token, calibrated_sensor_token,
                timestamp, height, width, file_name, file_format,
                prev, next
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            batch_size = 10000
            total_image_rows = 0
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                
                frame_data = []
                frame_records = []
                for row in rows:
                    token = blob_to_token(row[0])
                    frame_token = secrets.token_hex(16)  # 새로운 frame_token 생성
                    ego_pose_token = blob_to_token(row[3])
                    calibrated_sensor_token = blob_to_token(row[4])
                    timestamp = row[5]
                    height = 1080
                    width = 1920
                    file_name = row[6]
                    file_format = 'jpg'
                    prev_token = blob_to_token(row[2])
                    next_token = blob_to_token(row[1])
                    
                    # frame 테이블 데이터
                    frame_records.append((frame_token, None))
                    
                    # frame_data 테이블 데이터
                    frame_data.append((
                        token,
                        frame_token,
                        ego_pose_token,
                        calibrated_sensor_token,
                        timestamp,
                        height,
                        width,
                        file_name,
                        file_format,
                        prev_token,
                        next_token
                    ))
                
                # frame 테이블 삽입
                self.mysql_cur.executemany(frame_insert_query, frame_records)
                self.mysql_conn.commit()
                
                # frame_data 테이블 삽입
                self.mysql_cur.executemany(frame_data_insert_query, frame_data)
                self.mysql_conn.commit()
                total_image_rows += len(frame_data)
                print(f"Processed {len(frame_data)} image frame_data rows (Total: {total_image_rows})")
            
            print(f"Completed processing {total_image_rows} image frame_data rows from {self.db_file}")
            
            # 2. LiDAR PC 데이터 처리
            self.sqlite_cur.execute("""
                SELECT token, next_token, prev_token, ego_pose_token, 
                    lidar_token, timestamp, filename, scene_token
                FROM lidar_pc
            """)
            
            total_lidar_rows = 0
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                
                frame_data = []
                frame_records = []
                for row in rows:
                    token = blob_to_token(row[0])
                    frame_token = secrets.token_hex(16)  # 새로운 frame_token 생성
                    ego_pose_token = blob_to_token(row[3])
                    calibrated_sensor_token = blob_to_token(row[4])
                    timestamp = row[5]
                    height = None
                    width = None
                    file_name = row[6]
                    file_format = 'pcd'
                    prev_token = blob_to_token(row[2])
                    next_token = blob_to_token(row[1])
                    scene_token = blob_to_token(row[7])  # scene_token 추가
                    
                    # frame 테이블 데이터
                    frame_records.append((frame_token, scene_token))
                    
                    # frame_data 테이블 데이터
                    frame_data.append((
                        token,
                        frame_token,
                        ego_pose_token,
                        calibrated_sensor_token,
                        timestamp,
                        height,
                        width,
                        file_name,
                        file_format,
                        prev_token,
                        next_token
                    ))
                
                # frame 테이블 삽입
                self.mysql_cur.executemany(frame_insert_query, frame_records)
                self.mysql_conn.commit()
                
                # frame_data 테이블 삽입
                self.mysql_cur.executemany(frame_data_insert_query, frame_data)
                self.mysql_conn.commit()
                total_lidar_rows += len(frame_data)
                print(f"Processed {len(frame_data)} lidar frame_data rows (Total: {total_lidar_rows})")
            
            print(f"Completed processing {total_lidar_rows} lidar frame_data rows from {self.db_file}")
            print(f"Total frame_data rows processed: {total_image_rows + total_lidar_rows}")
                
        except Exception as e:
            print(f"Error processing frame_data in {self.db_file}: {e}")
            raise

    def process_sensors(self):
        try:
            sensor_rows = []
            cal_rows = []
            
            # Camera 처리
            self.sqlite_cur.execute("""
                SELECT token, channel,
                       translation, rotation,
                       intrinsic, distortion
                FROM camera
            """)
            for row in self.sqlite_cur.fetchall():
                cam_token = blob_to_token(row[0])
                channel = row[1]
                
                trans_obj = safe_pickle_loads(row[2])
                rot_obj = safe_pickle_loads(row[3])
                intr_obj = safe_pickle_loads(row[4])
                dist_obj = safe_pickle_loads(row[5])
                
                sensor_token = secrets.token_hex(16)
                sensor_rows.append((sensor_token, channel, "camera"))
                cal_rows.append((
                    cam_token,
                    sensor_token,
                    json.dumps(trans_obj),
                    json.dumps(rot_obj),
                    json.dumps(intr_obj),
                    None,
                    json.dumps(dist_obj)
                ))
            
            # LiDAR 처리
            self.sqlite_cur.execute("""
                SELECT token, channel,
                       translation, rotation
                FROM lidar
            """)
            for row in self.sqlite_cur.fetchall():
                lidar_token = blob_to_token(row[0])
                channel = row[1]
                
                trans_obj = safe_pickle_loads(row[2])
                rot_obj = safe_pickle_loads(row[3])
                
                sensor_token = secrets.token_hex(16)
                sensor_rows.append((sensor_token, channel, "lidar"))
                cal_rows.append((
                    lidar_token,
                    sensor_token,
                    json.dumps(trans_obj),
                    json.dumps(rot_obj),
                    None,
                    None,
                    None
                ))
            
            # 배치 삽입
            batch_size = 1000
            for i in range(0, len(sensor_rows), batch_size):
                batch = sensor_rows[i:i + batch_size]
                self.mysql_cur.executemany(
                    "INSERT INTO sensor (token, channel, modality) VALUES (%s, %s, %s)",
                    batch
                )
                self.mysql_conn.commit()
            
            for i in range(0, len(cal_rows), batch_size):
                batch = cal_rows[i:i + batch_size]
                self.mysql_cur.executemany(
                    """
                    INSERT INTO calibrated_sensor (
                        token, sensor_token,
                        translation, rotation,
                        camera_intrinsic, height_from_ground, distortion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    batch
                )
                self.mysql_conn.commit()
                
            print(f"Inserted {len(sensor_rows)} sensors and {len(cal_rows)} calibrated sensors from {self.db_file}")
            
        except Exception as e:
            print(f"Error processing sensors in {self.db_file}: {e}")
            raise

    def process_scenario(self):
        try:
            # scenario 데이터 가져오기
            self.sqlite_cur.execute("""
                SELECT token, type
                FROM scenario_tag
            """)
            
            insert_query = """
            INSERT INTO scenario (
                token, description
            ) VALUES (
                %s, %s
            )
            """
            
            batch_size = 10000
            total_rows = 0
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    description = row[1]  # type을 description으로 사용
                    
                    data.append((token, description))
                
                self.mysql_cur.executemany(insert_query, data)
                self.mysql_conn.commit()
                total_rows += len(data)
                print(f"Processed {len(data)} scenario rows (Total: {total_rows})")
            
            print(f"Completed processing {total_rows} scenario rows from {self.db_file}")
                
        except Exception as e:
            print(f"Error processing scenario in {self.db_file}: {e}")
            raise

    def process_scenarios_in_scene(self):
        try:
            # scenarios_in_scene 데이터 가져오기
            self.sqlite_cur.execute("""
                SELECT
                    st.token   AS scenario_tag_token,
                    sc.token   AS scene_token
                FROM
                    scenario_tag AS st
                    INNER JOIN lidar_pc AS lp
                        ON st.lidar_pc_token = lp.token
                    INNER JOIN scene AS sc
                        ON lp.scene_token = sc.token
            """)
            
            insert_query = """
            INSERT INTO scenarios_in_scene (
                scene_token,
                scenario_token
            ) VALUES (
                %s, %s
            )
            """
            
            batch_size = 10000
            total_rows = 0
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                data = []
                for row in rows:
                    scenario_token = blob_to_token(row[0])
                    scene_token = blob_to_token(row[1])
                    
                    data.append((scene_token, scenario_token))
                
                self.mysql_cur.executemany(insert_query, data)
                self.mysql_conn.commit()
                total_rows += len(data)
                print(f"Processed {len(data)} scenarios_in_scene rows (Total: {total_rows})")
            
            print(f"Completed processing {total_rows} scenarios_in_scene rows from {self.db_file}")
                
        except Exception as e:
            print(f"Error processing scenarios_in_scene in {self.db_file}: {e}")
            raise

    def process_log(self):
        try:
            # log 테이블 처리
            self.sqlite_cur.execute("""
                SELECT token, vehicle_name, timestamp, logfile, location
                FROM log
            """)
            
            insert_query = """
            INSERT INTO log (
                token, dataset_token, device, date_captured, location, logfile
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
            """
            
            batch_size = 10000
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    device = row[1]  # vehicle_name을 device로 사용
                    timestamp = convert_timestamp_to_datetime(row[2])
                    logfile = row[3]
                    location = row[4]
                    
                    data.append((
                        token,
                        self.dataset_token,
                        device,
                        timestamp,  # timestamp를 datetime으로 변환하여 date_captured로 사용
                        location,
                        logfile
                    ))
                
                self.mysql_cur.executemany(insert_query, data)
                self.mysql_conn.commit()
                print(f"Inserted {len(data)} log rows from {self.db_file}")
                
        except Exception as e:
            print(f"Error processing log in {self.db_file}: {e}")
            raise

    def process_scene(self):
        try:
            # scene 테이블 처리
            self.sqlite_cur.execute("""
                SELECT token, log_token, goal_ego_pose_token
                FROM scene
            """)
            
            insert_query = """
            INSERT INTO scene (
                token, log_token, base_path, goal_ego_pose_token
            ) VALUES (
                %s, %s, %s, %s
            )
            """
            
            batch_size = 10000
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    log_token = blob_to_token(row[1])
                    goal_ego_pose_token = blob_to_token(row[2])
                    
                    data.append((
                        token,
                        log_token,
                        "nuplan-v1.1_mini",
                        goal_ego_pose_token
                    ))
                
                self.mysql_cur.executemany(insert_query, data)
                self.mysql_conn.commit()
                print(f"Inserted {len(data)} scene rows from {self.db_file}")
                
        except Exception as e:
            print(f"Error processing scene in {self.db_file}: {e}")
            raise

    def process_frame_annotation(self):
        try:
            # 1. lidar_box 데이터 가져오기
            self.sqlite_cur.execute("""
                SELECT token, lidar_pc_token, track_token, next_token, prev_token,
                    x, y, z, width, length, height, vx, vy, vz, yaw
                FROM lidar_box
            """)

            # lidar_box 테이블에 데이터 삽입
            insert_lidar_box_query = """
            INSERT INTO lidar_box (
                token, lidar_pc_token, track_token, next_token, prev_token,
                translation_center, size_wlh, velocity, yaw
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            batch_size = 50000
            while True:
                rows = self.sqlite_cur.fetchmany(batch_size)
                if not rows:
                    break

                data = []
                for row in rows:
                    token = blob_to_token(row[0])
                    lidar_pc_token = blob_to_token(row[1])
                    track_token = blob_to_token(row[2])
                    next_token = blob_to_token(row[3])
                    prev_token = blob_to_token(row[4])

                    translation = json.dumps([row[5], row[6], row[7]])
                    size = json.dumps([row[8], row[9], row[10]])
                    velocity = json.dumps([row[11], row[12], row[13]])
                    yaw = json.dumps(row[14])

                    data.append((
                        token,
                        lidar_pc_token,
                        track_token,
                        next_token,
                        prev_token,
                        translation,
                        size,
                        velocity,
                        yaw
                    ))

                self.mysql_cur.executemany(insert_lidar_box_query, data)
                self.mysql_conn.commit()
                print(f"Inserted {len(data)} lidar_box rows from {self.db_file}")

            # 2. track 테이블과 lidar_box 조인 결과를 instance 테이블에 삽입 (성능 측정 추가)
            join_start_time = time.time()
            self.mysql_cur.execute("""
                INSERT INTO instance (
                    token,
                    category_token,
                    size,
                    num_annotations,
                    first_frame_annotation_token,
                    last_frame_annotation_token
                )
                SELECT
                    t.token AS token,
                    t.category_token AS category_token,
                    t.size AS size,
                    COALESCE(lb_cnt.num_lidar_box, 0) AS num_annotations,
                    f.token AS first_frame_annotation_token,
                    l.token AS last_frame_annotation_token
                FROM track t
                LEFT JOIN (
                    SELECT
                        track_token,
                        COUNT(*) AS num_lidar_box
                    FROM lidar_box
                    GROUP BY track_token
                ) AS lb_cnt
                    ON lb_cnt.track_token = t.token
                LEFT JOIN lidar_box AS f
                    ON f.track_token = t.token
                    AND f.prev_token IS NULL
                LEFT JOIN lidar_box AS l
                    ON l.track_token = t.token
                    AND l.next_token IS NULL
            """)
            self.mysql_conn.commit()
            join_end_time = time.time()
            print(f"Dumped joined data from track and lidar_box into instance for {self.db_file} in {join_end_time - join_start_time:.3f}초")

            # 3. frame_annotation 테이블에 데이터 삽입 (성능 측정 추가)
            frame_ann_start = time.time()
            self.mysql_cur.execute("""
                INSERT INTO frame_annotation (
                    token,
                    frame_token,
                    frame_data_token,
                    instance_token,
                    annotation_type_name,
                    prev,
                    next,
                    translation_center,
                    size_wlh,
                    rotation_orientation,
                    velocity
                )
                SELECT
                    lb.token AS token,
                    fd.frame_token AS frame_token,
                    lb.lidar_pc_token AS frame_data_token,
                    lb.track_token AS instance_token,
                    'bbox_pcd3d' AS annotation_type_name,
                    lb.prev_token AS prev,
                    lb.next_token AS next,
                    lb.translation_center AS translation_center,
                    lb.size_wlh AS size_wlh,
                    lb.yaw AS rotation_orientation,
                    lb.velocity AS velocity
                FROM lidar_box lb
                JOIN frame_data fd ON lb.lidar_pc_token = fd.token
            """)
            self.mysql_conn.commit()
            frame_ann_end = time.time()
            print(f"Transferred data from lidar_box to frame_annotation for {self.db_file} in {frame_ann_end - frame_ann_start:.3f}초")

            # 4. lidar_box 테이블 비우기
            self.mysql_cur.execute("TRUNCATE TABLE lidar_box")
            self.mysql_conn.commit()
            print("Truncated lidar_box table after processing")

            # 5. track 테이블 비우기
            self.mysql_cur.execute("TRUNCATE TABLE track")
            self.mysql_conn.commit()
            print("Truncated track table after processing")

        except Exception as e:
            print(f"Error processing frame_annotation in {self.db_file}: {e}")
            raise


def process_all_db_files():
    """모든 DB 파일을 처리하는 메인 함수"""
    db_files = glob.glob(os.path.join(DATA_BASE_PATH, "*.db"))
    total_files = len(db_files)
    
    # dataset 테이블 처리 - 한 번만 실행
    mysql_hook = MySqlHook(mysql_conn_id='aws_mysql', schema='nuPlan')
    with mysql_hook.get_conn() as conn:
        with conn.cursor() as cur:
            dataset_token = secrets.token_hex(16)
            cur.execute(
                "INSERT INTO dataset (token, name) VALUES (%s, %s)",
                (dataset_token, "nuPlan")
            )
            conn.commit()
            print("Created dataset record with token:", dataset_token)
    
    for idx, db_file in enumerate(db_files, 1):
        print(f"\nProcessing file {idx}/{total_files}: {db_file}")
        try:
            with NuPlanDataProcessor(db_file, dataset_token) as processor:
                processor.process_log()
                processor.process_scene()
                processor.process_ego_pose()
                processor.process_category()
                processor.process_instance()
                processor.process_frame_data()
                processor.process_frame_annotation()
                processor.process_sensors()
                processor.process_scenario()
                processor.process_scenarios_in_scene()  # scenarios_in_scene 처리 추가
            print(f"Successfully processed {db_file}")
        except Exception as e:
            print(f"Error processing {db_file}: {e}")
            continue


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
            SELECT * FROM nuPlan.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()
    

with DAG(
    dag_id='nuplan_data_insert',
    default_args=default_args,
    description='Insert nuPlan dataset into MySQL database',
    schedule=None
) as dag:

    start = EmptyOperator(task_id='start')
    
    process_data = PythonOperator(
        task_id='process_all_db_files',
        python_callable=process_all_db_files
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process_data >> end 