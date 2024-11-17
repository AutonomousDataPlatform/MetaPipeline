import boto3
import pymysql
import pymysql.cursors
import decimal
import json
import dbinfo

connection = pymysql.connect(host = dbinfo.db_host, port = 23306, user = dbinfo.db_username, passwd = dbinfo.db_password, db = dbinfo.db_name)

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Decimal을 float으로 변환
    raise TypeError

def s3_json_upload(result, bucket, json_file_name):
    content = json.dumps(result, ensure_ascii = False, indent = 2, default = default).encode('UTF-8')
    
    s3 = boto3.client('s3', region_name = 'ap-northeast-2')
    s3.put_object(Bucket = bucket, Key = 'test/' + json_file_name, Body = content)

def lambda_handler(event, context):
    bucket = 'adds-json'

    cursor = connection.cursor(pymysql.cursors.DictCursor)

    json_file_name = 'annotation_type.json'
    cursor.execute("SELECT * FROM annotation_type_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'visibility.json'
    cursor.execute("SELECT * FROM visibility_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'lidarseg.json'
    cursor.execute("SELECT * FROM  lidarseg_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'sensor.json'
    cursor.execute("SELECT * FROM sensor_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'instance.json'
    cursor.execute("SELECT * FROM instance_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'attribute.json'
    cursor.execute("SELECT * FROM attribute_temp")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'frame.json'
    cursor.execute('SELECT * FROM frame_temp')
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'log.json'
    cursor.execute('SELECT * FROM log_temp')
    results = cursor.fetchall()

    for row in results:
        if row['device_wlh']:
            row['device_wlh'] = json.loads(row['device_wlh'])

    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'calibrated_sensor.json'
    cursor.execute('SELECT * FROM calibrated_sensor_temp')
    results = cursor.fetchall()

    for row in results:
        row['translation'] = json.loads(row['translation'])
        row['rotation'] = json.loads(row['rotation'])

        if row['camera_intrinsic']:
            row['camera_intrinsic'] = json.loads(row['camera_intrinsic'])

    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'category.json'
    cursor.execute('SELECT * FROM category_temp')
    results = cursor.fetchall()

    for row in results:
        if row['default_scale']:
            row['default_scale']=json.loads(row['default_scale'])

    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'ego_pose.json'
    cursor.execute('SELECT * FROM ego_pose_temp')
    results = cursor.fetchall()

    for row in results:
        row['translation']=json.loads(row['translation'])
        row['rotation']=json.loads(row['rotation'])

        if row['coordinate_system']:
            row['coordinate_system']=json.loads(row['coordinate_system'])

    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'frame_data.json'
    cursor.execute("SELECT * FROM frame_data_temp")
    results = cursor.fetchall()

    for row in results:
        if row['is_key_frame']==0:
            row['is_key_frame']=False
        elif row['is_key_frame']==1:
            row['is_key_frame']=True

    s3_json_upload(results, bucket, json_file_name)

    json_file_name = 'dataset.json'
    cursor.execute("SELECT sd.token, sd.log_token, st.name, st.description , sd.num_frames, sd.first_frame_token, sd.last_frame_token, sd.base_path FROM scenario_temp st JOIN (SELECT * FROM scenarios_in_dataset sid JOIN dataset_temp dt ON sid.dataset_token = dt.token) AS sd ON sd.scenario_name = st.name")
    results = cursor.fetchall()
    s3_json_upload(results, bucket, json_file_name)

    # map json
    json_file_name = 'map.json'
    cursor.execute("SELECT * FROM map_temp")
    map_results = cursor.fetchall()
    map_data = {row['token']: row for row in map_results}

    cursor.execute("SELECT * FROM log_map")
    log_map_results = cursor.fetchall()

    for token in map_data:
        map_data[token]['log_tokens'] = None

    for row in log_map_results:
        map_token = row['map_token']
        if map_token in map_data:
            if map_data[map_token]['log_tokens'] is None:
                map_data[map_token]['log_tokens'] = []
            map_data[map_token]['log_tokens'].append(row['log_token'])

    final_data = list(map_data.values())

    s3_json_upload(final_data, bucket, json_file_name)

    cursor.close()
    connection.close()