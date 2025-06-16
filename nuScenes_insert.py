import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json, ijson
import pymysql
import os
from airflow.decorators import task


default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


def annotation_type_insert():

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    query = "INSERT IGNORE INTO annotation_type (name, description) VALUES ('nuScenes_annotation', '카메라 및 라이다, 레이다 3가지에 대한 통합 어노테이션 정보')"
    cur.execute(query)
    conn.commit()

    cur.close()
    conn.close()


def attribute_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_attribute.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/attribute.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:

            json_data = json.load(f)
            data = []
            
            for attribute_data in json_data:
                token = attribute_data['token']
                name = attribute_data['name']
                description = attribute_data['description']

                data.append((token, name, description))

            query = "INSERT IGNORE INTO attribute (token, name, description) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


def calibrated_sensor_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_calibrated_sensor.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/calibrated_sensor.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:

            json_data = json.load(f)
            data = []
            
            for calibrated_sensor_data in json_data:

                token = calibrated_sensor_data['token']
                sensor_token = calibrated_sensor_data['sensor_token']
                translation = calibrated_sensor_data['translation']
                rotation = calibrated_sensor_data['rotation']
                height_from_ground = calibrated_sensor_data['translation'][2]
                
                if calibrated_sensor_data['camera_intrinsic']:
                    camera_intrinsic = calibrated_sensor_data['camera_intrinsic']

                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), json.dumps(camera_intrinsic), height_from_ground))

                else:
                    camera_intrinsic = None
                    
                    data.append((token, sensor_token, json.dumps(translation), json.dumps(rotation), camera_intrinsic, height_from_ground))
                                

            query = "INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation, rotation, camera_intrinsic, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s)"


            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


def ego_pose_insert():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    remote_path = '/data/nuScenes/v1.0-trainval'
    local_path = 'nuScenes_ego_pose.json'

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE nuScenes")

    with hook.get_conn() as sftp_client:
        file_path = remote_path + '/ego_pose.json'
        sftp_client.get(file_path, local_path)

        print(f"ego_pose.json 다운로드 완료")

        with open(local_path) as f:
            json_data = json.load(f)
            data = []

            print(f"ego_pose 배열 길이 ::::::: {len(json_data)}")
            print("첫 5개 token:", [row["token"] for row in json_data[:5]])

            for i in range(len(json_data)):
                ego_pose_data = json_data[i]

                token = ego_pose_data['token']
                timestamp = str(ego_pose_data['timestamp'])
                translation = json.dumps(ego_pose_data['translation'])
                rotation = json.dumps(ego_pose_data['rotation'])
                pose_type = "vehicle"

                data.append((token, timestamp, translation, rotation, pose_type))

                if i > 0 and i % 50000 == 0:
                    print(f"executemany {i // 50000} start")
                    query = """
                        INSERT INTO ego_pose
                        (token, timestamp, translation, rotation, pose_type)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cur.executemany(query, data)
                    conn.commit()
                    print(f"executemany {i // 50000} end")
                    data = []

            if data:
                print("executemany last start")
                cur.executemany(query, data)
                conn.commit()
                print("executemany last end")

    cur.close()
    conn.close()
    os.remove(local_path)



def instance_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_instance.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/instance.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for instance_data in json_data:

                token = instance_data['token']
                category_token = instance_data['category_token']
                first_frame_annotation_token = instance_data['first_annotation_token']
                last_frame_annotation_token = instance_data['last_annotation_token']
                num_annotations = instance_data['nbr_annotations']
                
                data.append((token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations))

            query = "INSERT IGNORE INTO instance (token, category_token, first_frame_annotation_token, last_frame_annotation_token, num_annotations) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(local_path)


def category_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes_lidarseg/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_category.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/category.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)
            data = []
            
            for category_data in json_data:
                
                token = category_data['token']
                name = category_data['name']
                description = category_data['description']
                index = category_data['index']
                
                data.append((token, name, description, index))

            query = "INSERT IGNORE INTO category (token, name, description, `index`) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


def lidarseg_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes_lidarseg/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_lidarseg_map.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/lidarseg.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:

            json_data = json.load(f)
            data = []

            for lidarseg_data in json_data:

                token = lidarseg_data['token']
                frame_data_token = lidarseg_data['sample_data_token']
                filename = lidarseg_data['filename']
                
                data.append((token, frame_data_token, filename))

            query = "INSERT IGNORE INTO lidarseg (token, frame_data_token, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(local_path)


def dataset_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_dataset_scene.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/scene.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for scene_data in json_data:
                token = scene_data['token']
                type = 'nuScenes'
                log_token = scene_data['log_token']
                base_path = None
                num_frames = scene_data['nbr_samples']
                first_frame_token = scene_data['first_sample_token']
                last_frame_token = scene_data['last_sample_token']
                
                data.append((token, type, log_token, base_path, num_frames, first_frame_token,last_frame_token))


            query = "INSERT IGNORE INTO dataset (token, type, log_token, base_path, num_frames, first_frame_token,last_frame_token) VALUES (%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


def log_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_log.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/log.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)
            data = []
            
            for log_data in json_data:
                token = log_data['token']
                device = log_data['vehicle']
                date_captured = log_data['date_captured']
                location = log_data['location']
                logfile = log_data['logfile']
                
                
                data.append((token, device, date_captured, location, logfile))

            query = "INSERT IGNORE INTO log (token, device, date_captured, location, logfile) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


def map_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_map_map.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/map.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for map_data in json_data:

                token = map_data['token']
                category = map_data['category']
                filename = map_data['filename']
                
                data.append((token, category, filename))

            query = "INSERT IGNORE INTO map (token, category, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(local_path)


def log_map_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_log_map_map.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/map.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for map_data in json_data:
                map_token = map_data['token']
                log_tokens = map_data['log_tokens']

                for log_token in log_tokens:
                    data.append((log_token, map_token))
            

            query = "INSERT IGNORE INTO log_map (log_token, map_token) VALUES (%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(local_path)


def frame_annotation_insert():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    remote_path = '/data/nuScenes/v1.0-trainval'
    local_path = 'nuScenes_frame_annotation.json'   # 기존과 동일

    # ── MySQL 연결 ───────────────────────────────────
    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE nuScenes")

    with hook.get_conn() as sftp_client:
        file_path = remote_path + '/sample_annotation.json'
        sftp_client.get(file_path, local_path)
    
    print("sample_annotation.json 다운로드 완료")

    query = """
        INSERT IGNORE INTO frame_annotation
        (token, frame_token, instance_token, visibility_token,
         annotation_type_name, size_wlh, translation_center,
         rotation_orientation, num_lidar_pts, num_radar_pts,
         attribute_tokens, prev, next)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data, BATCH_SIZE = [], 50_000

    # ── json.load 대신 ijson 스트리밍 ─────────────────
    with open(local_path, 'rb') as f:  # rb 모드 필수
        for i, ann in enumerate(ijson.items(f, 'item'), 1):
            token = ann['token']
            frame_token = ann['sample_token']
            instance_token = ann['instance_token']
            visibility_token = ann['visibility_token']
            annotation_type = 'nuScenes_annotation'

            size_wlh = json.dumps([float(x) for x in ann['size']])
            translation_center = json.dumps([float(x) for x in ann['translation']])
            rotation_orientation = json.dumps([float(x) for x in ann['rotation']])

            num_lidar_pts = ann['num_lidar_pts']
            num_radar_pts = ann['num_radar_pts']

            attribute_tokens = json.dumps(ann['attribute_tokens']) if ann['attribute_tokens'] else None
            prev = ann['prev'].replace("-", "") if ann['prev'] else None
            next = ann['next'].replace("-", "") if ann['next'] else None

            data.append((
                token, frame_token, instance_token, visibility_token, annotation_type,
                size_wlh, translation_center, rotation_orientation,
                num_lidar_pts, num_radar_pts, attribute_tokens, prev, next
            ))

            if i > 0 and i % BATCH_SIZE == 0:
                print(f"executemany {i // BATCH_SIZE} start")
                cur.executemany(query, data)
                conn.commit()
                print(f"executemany {i // BATCH_SIZE} end")
                data = []          # 기존과 동일하게 리스트 초기화

    # 남은 레코드 처리(기존 로직 유지)
    if data:
        print("executemany last start")
        cur.executemany(query, data)
        conn.commit()
        print("executemany last end")

    cur.close()
    conn.close()
    os.remove(local_path)


def frame_data_insert():
    hook = SFTPHook(ssh_conn_id="eun_sftp")
    remote_path = '/data/nuScenes/v1.0-trainval'
    local_path  = 'nuScenes_frame_data_sample_data.json' 

    mysql_hook = MySqlHook.get_hook(conn_id='aws_mysql')
    conn       = mysql_hook.get_conn()
    cur        = conn.cursor()
    cur.execute("USE nuScenes")

    with hook.get_conn() as sftp_client:
        sftp_client.get(remote_path + '/sample_data.json', local_path)
    print("sample_data.json 다운로드 완료")

    query = """
        INSERT IGNORE INTO frame_data
        (token, frame_token, ego_pose_token, calibrated_sensor_token,
         timestamp, is_key_frame, height, width,
         file_name, file_format, prev, next)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    data, BATCH_SIZE = [], 50_000            # 변수명 data 유지

    # ── json.load → ijson 스트리밍으로만 변경 ───────────
    with open(local_path, 'rb') as f:        # rb 모드
        for i, sample_data in enumerate(ijson.items(f, 'item'), 1):
            token = sample_data['token']
            frame_token = sample_data['sample_token']
            ego_pose_token = sample_data['ego_pose_token']
            calibrated_sensor_token = sample_data['calibrated_sensor_token']
            timestamp = str(sample_data['timestamp'])
            is_key_frame = sample_data['is_key_frame']
            height = sample_data['height']
            width = sample_data['width']
            file_name = sample_data['filename']
            file_format = sample_data['fileformat']
            prev = sample_data['prev'] or None
            next = sample_data['next'] or None

            data.append((
                token, frame_token, ego_pose_token, calibrated_sensor_token,
                timestamp, is_key_frame, height, width,
                file_name, file_format, prev, next
            ))

            if i % BATCH_SIZE == 0:
                print(f"executemany {i // BATCH_SIZE} start")
                cur.executemany(query, data)
                conn.commit()
                print(f"executemany {i // BATCH_SIZE} end")
                data = []            # 리스트 초기화 (기존 로직 유지)

    if data:
        print("executemany last start")
        cur.executemany(query, data)
        conn.commit()
        print("executemany last end")

    cur.close()
    conn.close()
    os.remove(local_path)



def frame_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuSceens_frame_sample.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/sample.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for sample_data in json_data:
                token = sample_data['token']
                timestamp = str(sample_data['timestamp'])

                prev = sample_data['prev']
                if prev == "":
                    prev = None

                next = sample_data['next']
                if next == "":
                    next = None
                
                dataset_token = sample_data['scene_token']
                
                data.append((token, timestamp, dataset_token, prev, next))

            query = "INSERT IGNORE INTO frame (token, timestamp, dataset_token, prev, next) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(local_path)


def scenario_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_scenario_scene.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/scene.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for scene_data in json_data:
                token = scene_data['token']
                description = scene_data['description']

                data.append((token, description))

            query = "INSERT IGNORE INTO scenario (token, description) VALUES (%s, %s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(local_path)


def scenarios_in_dataset_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_scenarios_in_dataset.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/scene.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)

            data = []

            for scene_data in json_data:

                dataset_token = scene_data['token']
                scene_token = scene_data['token']

                data.append((dataset_token, scene_token))


            query = "INSERT IGNORE INTO scenarios_in_dataset (dataset_token, scenario_token) VALUES (%s,%s)"

            cur.executemany(query,data)
            conn.commit()
    

    cur.close()
    conn.close()
    os.remove(local_path)


def sensor_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_sensor.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/sensor.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:

            json_data = json.load(f)
            data = []
            
            for sensor_data in json_data:
        
                token = sensor_data['token']
                channel = sensor_data['channel']
                modality = sensor_data['modality']

                data.append((token, channel, modality))

            query = "INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


def visibility_insert():
    #sftp 접속 정보
    hook = SFTPHook(ssh_conn_id = "eun_sftp")

    remote_path = '/data/nuScenes/v1.0-trainval'
    #local 에 저장할 때 파일 이름
    local_path = 'nuScenes_visibility.json'

    #mysql 접속 정보
    mysql_hook = MySqlHook.get_hook(conn_id = 'aws_mysql')
    conn = mysql_hook.get_conn()
    cur = conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:

        file_path = remote_path + '/visibility.json'
        sftp_client.get(file_path, local_path)

        with open(local_path) as f:
            json_data = json.load(f)
            data = []
            
            for visibility_data in json_data:
        
                token = visibility_data['token']
                level = visibility_data['level']
                description = visibility_data['description']

                data.append((token, level, description))

            query = "INSERT IGNORE INTO visibility (token, level, description) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()
    os.remove(local_path)


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
            db = 'nuScenes',
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
        db = 'nuScenes',
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
        'calibrated_sensor',
        'ego_pose',
        'instance',
        'category',
        'lidarseg',
        'dataset',
        'log',
        'map',
        'log_map',
        'frame_annotation',
        'frame_data',
        'frame',
        'scenario',
        'scenarios_in_dataset',
        'sensor',
        'visibility'
    ]

    for table in tables:
        print(f"Copying table: {table}")
        query = f"""
            INSERT IGNORE INTO adds.{table}
            SELECT * FROM nuScenes.{table}
        """
        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()


with DAG('nuScenes_insert',
        default_args = default_args,
        schedule_interval = None,
        )as dag:
    
    annotation_type_insert_task = PythonOperator(task_id = 'annotation_type_insert', python_callable = annotation_type_insert)
    attribute_insert_task = PythonOperator(task_id = 'attribute_insert', python_callable = attribute_insert)
    calibrated_sensor_insert_task = PythonOperator(task_id = 'calibrated_sensor_insert', python_callable = calibrated_sensor_insert)
    ego_pose_insert_task = PythonOperator(task_id = 'ego_pose_insert', python_callable = ego_pose_insert)
    instance_insert_task = PythonOperator(task_id = 'instance_insert', python_callable = instance_insert)
    category_insert_task = PythonOperator(task_id = 'category_insert', python_callable = category_insert)
    lidarseg_insert_task = PythonOperator(task_id = 'lidarseg_insert', python_callable = lidarseg_insert)
    dataset_insert_task = PythonOperator(task_id = 'dataset_insert', python_callable = dataset_insert)
    log_insert_task = PythonOperator(task_id = 'log_insert', python_callable = log_insert)
    map_insert_insert = PythonOperator(task_id = 'map_insert', python_callable = map_insert)
    log_map_insert_insert = PythonOperator(task_id = 'log_map_insert', python_callable = log_map_insert)
    frame_annotation_insert_task = PythonOperator(task_id = 'frame_annotation_insert', python_callable = frame_annotation_insert)
    frame_data_insert_task = PythonOperator(task_id = 'sample_data_insert', python_callable = frame_data_insert)
    frame_insert_task = PythonOperator(task_id = 'frame_insert', python_callable = frame_insert)
    scenario_insert_task = PythonOperator(task_id = 'scenario_insert', python_callable = scenario_insert)
    scenarios_in_dataset_insert_task = PythonOperator(task_id = 'scenarios_in_dataset_insert', python_callable = scenarios_in_dataset_insert)
    sensor_insert_task = PythonOperator(task_id = 'sensor_insert', python_callable = sensor_insert)
    visibility_insert_task = PythonOperator(task_id = 'visibility_insert', python_callable = visibility_insert)
    copy_to_adds_task = PythonOperator(task_id = 'copy_to_adds', python_callable = copy_to_adds)

    chunk_params = get_chunk_parameters()

    attribute_frame_annotation_insert_task = PythonOperator.partial(task_id = 'process_batch', python_callable = attribute_frame_annotation_insert).expand(op_kwargs = chunk_params)


[annotation_type_insert_task, attribute_insert_task, calibrated_sensor_insert_task, instance_insert_task, category_insert_task, lidarseg_insert_task, dataset_insert_task, log_insert_task, map_insert_insert, log_map_insert_insert, frame_insert_task, scenario_insert_task, scenarios_in_dataset_insert_task, sensor_insert_task, visibility_insert_task] >> ego_pose_insert_task >> frame_data_insert_task >> frame_annotation_insert_task >> chunk_params >> attribute_frame_annotation_insert_task >> copy_to_adds_task