import boto3
import pymysql
import pymysql.cursors
import decimal
import json
import dbinfo

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Decimal을 float으로 변환
    raise TypeError

def s3_json_upload(result, bucket, folder, json_file_name):
    content = json.dumps(result, ensure_ascii=False, indent=2, default=default).encode('UTF-8')
    s3 = boto3.client('s3', region_name='ap-northeast-2')
    s3.put_object(Bucket=bucket, Key=f"{folder}/{json_file_name}", Body=content)

def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        connection = pymysql.connect(
            host=dbinfo.db_host,
            port=23306,
            user=dbinfo.db_username,
            passwd=dbinfo.db_password,
            db=dbinfo.db_name
        )
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        bucket = 'adds-json'
        folder = event.get('executionArn')

        for table, filename in [
            ('annotation_type_temp', 'annotation_type.json'),
            ('visibility_temp', 'visibility.json'),
            ('lidarseg_temp', 'lidarseg.json'),
            ('sensor_temp', 'sensor.json'),
            ('instance_temp', 'instance.json'),
            ('attribute_temp', 'attribute.json'),
            ('frame_temp', 'frame.json')
        ]:
            cursor.execute(f"SELECT * FROM {table}")
            results = cursor.fetchall()
            s3_json_upload(results, bucket, folder, filename)

        # log
        cursor.execute('SELECT * FROM log_temp')
        results = cursor.fetchall()
        for row in results:
            if row['device_wlh']:
                row['device_wlh'] = json.loads(row['device_wlh'])
        s3_json_upload(results, bucket, folder, 'log.json')

        # calibrated_sensor
        cursor.execute('SELECT * FROM calibrated_sensor_temp')
        results = cursor.fetchall()
        for row in results:
            if row['translation']:
                row['translation'] = json.loads(row['translation'])
            if row['rotation']:
                row['rotation'] = json.loads(row['rotation'])
            if row['camera_intrinsic']:
                row['camera_intrinsic'] = json.loads(row['camera_intrinsic'])
        s3_json_upload(results, bucket, folder, 'calibrated_sensor.json')

        # category
        cursor.execute('SELECT * FROM category_temp')
        results = cursor.fetchall()
        for row in results:
            if row['default_scale']:
                row['default_scale'] = json.loads(row['default_scale'])
        s3_json_upload(results, bucket, folder, 'category.json')

        # ego_pose
        cursor.execute('SELECT * FROM ego_pose_temp')
        results = cursor.fetchall()
        for row in results:
            if row['translation']:
                row['translation'] = json.loads(row['translation'])
            if row['rotation']:
                row['rotation'] = json.loads(row['rotation'])
            if row['coordinate_system']:
                row['coordinate_system'] = json.loads(row['coordinate_system'])
        s3_json_upload(results, bucket, folder, 'ego_pose.json')

        # frame_data
        cursor.execute("SELECT * FROM frame_data_temp")
        results = cursor.fetchall()
        for row in results:
            row['is_key_frame'] = bool(row['is_key_frame'])
        s3_json_upload(results, bucket, folder, 'frame_data.json')

        # scene
        # NOTE: ss => scenario, scene 조인 테이블, srt => scenario 테이블, st => scene 테이블
        cursor.execute("""
            SELECT ss.token, ss.log_token, srt.token, srt.description,
                   ss.num_frames, ss.first_frame_token, ss.last_frame_token, ss.base_path
            FROM scenario_temp srt
            JOIN (
                SELECT * FROM scenarios_in_scene sis
                JOIN scene_temp st ON sis.scene_token = st.token
            ) AS ss ON ss.scenario_token = srt.token
        """)
        results = cursor.fetchall()
        s3_json_upload(results, bucket, folder, 'scene.json')

        # map
        cursor.execute("SELECT * FROM map_temp")
        map_results = cursor.fetchall()
        map_data = {row['token']: row for row in map_results}
        for token in map_data:
            map_data[token]['log_tokens'] = None

        cursor.execute("SELECT * FROM log_map")
        log_map_results = cursor.fetchall()
        for row in log_map_results:
            map_token = row['map_token']
            if map_token in map_data:
                if map_data[map_token]['log_tokens'] is None:
                    map_data[map_token]['log_tokens'] = []
                map_data[map_token]['log_tokens'].append(row['log_token'])

        s3_json_upload(list(map_data.values()), bucket, folder, 'map.json')

    except Exception as e:
        print("[ERROR] lambda_handler failed:", e)
        raise e

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print("[WARNING] cursor close failed:", e)
        if connection:
            try:
                connection.close()
            except Exception as e:
                print("[WARNING] connection close failed:", e)
