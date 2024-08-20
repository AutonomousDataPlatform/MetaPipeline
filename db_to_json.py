import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import pymysql
import pymysql.cursors

default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}

def unified_annotation_type_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM annotation_type")

    results=cur.fetchall()

    json_file_name='unified_json/unified_annotation_type.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()

def unified_visibility_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM visibility")

    results=cur.fetchall()

    json_file_name='unified_json/unified_visibility.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_lidarseg_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM lidarseg")

    results=cur.fetchall()

    json_file_name='unified_json/unified_lidarseg.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_sensor_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM sensor")

    results=cur.fetchall()

    json_file_name='unified_json/unified_sensor.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_instance_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM instance")

    results=cur.fetchall()

    json_file_name='unified_json/unified_instance.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()



def unified_attribute_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM attribute")

    results=cur.fetchall()

    json_file_name='unified_json/unified_attribute.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_frame_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM frame")

    results=cur.fetchall()

    json_file_name='unified_json/unified_frame.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_log_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    cur=conn.cursor()
    cur.execute("SELECT * FROM log")

    results=cur.fetchall()

    for row in results:

        if row['driving_distance']:
            row['driving_distance']=float(row['driving_distance'])

        if row['device_wlh']:
            row['device_wlh']=json.loads(row['device_wlh'])
        
    
    json_file_name='unified_json/unified_log.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_calibrated_sensor_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT * FROM calibrated_sensor")

    results=cur.fetchall()

    for row in results:
        row['translation']=json.loads(row['translation'])
        row['rotation']=json.loads(row['rotation'])

        if row['camera_intrinsic']:
            row['camera_intrinsic']=json.loads(row['camera_intrinsic'])
        
        row['height_from_ground']=float(row['height_from_ground'])


    json_file_name='unified_json/unified_calibrated_sensor.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_category_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT * FROM category")

    results=cur.fetchall()

    for row in results:

        if row['default_scale']:
            row['default_scale']=json.loads(row['default_scale'])
        

    json_file_name='unified_json/unified_category.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_ego_pose_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT * FROM ego_pose")

    results=cur.fetchall()

    for row in results:

        row['translation']=json.loads(row['translation'])
        row['rotation']=json.loads(row['rotation'])

        if row['coordinate_system']:
            row['coordinate_system']=json.loads(row['coordinate_system'])
        

    json_file_name='unified_json/unified_ego_pose.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_frame_data_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT * FROM frame_data")

    results=cur.fetchall()

    for row in results:
        
        if row['is_key_frame']==0:
            row['is_key_frame']=False
        elif row['is_key_frame']==1:
            row['is_key_frame']=True
        

    json_file_name='unified_json/unified_frame_data.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_dataset_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT sd.token, sd.log_token, s.name, s.description , sd.num_frames, sd.first_frame_token, sd.last_frame_token, sd.base_path from scenario s join (SELECT * from scenarios_in_dataset sid join dataset d on sid.dataset_token = d.token) as sd on sd.scenario_name = s.name")

    results=cur.fetchall()
        
    json_file_name='unified_json/unified_dataset.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()



def unified_map_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )
   
    cur=conn.cursor()
    cur.execute("SELECT * FROM map")

    map_results=cur.fetchall()

    map_data={row['token']: row for row in map_results}

    cur.execute("SELECT * FROM log_map")
    log_map_results=cur.fetchall()


    for token in map_data:
        map_data[token]['log_tokens'] = None

    for row in log_map_results:
        map_token=row['map_token']
        if map_token in map_data:
            if map_data[map_token]['log_tokens'] is None:
                map_data[map_token]['log_tokens']=[]
            map_data[map_token]['log_tokens'].append(row['log_token'])

    
    final_data=list(map_data.values())


    
    json_file_name='unified_json/unified_map.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(final_data, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()


def unified_frame_annotation_json():

    conn_info=MySqlHook.get_connection(conn_id='aws_mysql')

    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor
    )

    page_size=200000
    offset=0

    final_data=[]

    while True:

        cur=conn.cursor()
        cur.execute(f"SELECT * FROM frame_annotation LIMIT {offset}, {page_size}")
        frame_annotation_results=cur.fetchall()

        if not frame_annotation_results:
            break

        for row in frame_annotation_results:

            if row['annotation_type_name']=='bbox_pcd3d':

                row['geometry']={}
                row['geometry']['wlh']=json.loads(row['size_wlh'])
                row['geometry']['center']=json.loads(row['translation_center'])
                row['geometry']['orientation']=json.loads(row['rotation_orientation'])
                del row['size_wlh']
                del row['translation_center']
                del row['rotation_orientation']
                del row['bbox_image3d']

            elif row['annotation_type_name']=='bbox_image3d':

                row['geometry']={}
                row['geometry']=json.loads(row['bbox_image3d'])
                del row['size_wlh']
                del row['translation_center']
                del row['rotation_orientation']
                del row['bbox_image3d']

            elif row['annotation_type_name']=='nuScenes_annotation':

                row['geometry']={}
                row['geometry']['size']=json.loads(row['size_wlh'])
                row['geometry']['translation']=json.loads(row['translation_center'])
                row['geometry']['rotation']=json.loads(row['rotation_orientation'])
                del row['size_wlh']
                del row['translation_center']
                del row['rotation_orientation']
                del row['bbox_image3d']


            final_data.append(row)
        
        cur.close()
        print(len(final_data))

        offset += page_size

    
    json_file_name='unified_json/unified_frame_annotation.json'

    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(final_data, json_file, ensure_ascii=False, indent=2)


    cur.close()
    conn.close()



with DAG('db_to_json',
         default_args=default_args,
         schedule_interval=None,
         )as dag:
    unified_annotation_type_json=PythonOperator(task_id='unified_annotation_type_json', python_callable=unified_annotation_type_json)
    unified_visibility_json=PythonOperator(task_id='unified_visibility_json', python_callable=unified_visibility_json)
    unified_lidarseg_json=PythonOperator(task_id='unified_lidarseg_json', python_callable=unified_lidarseg_json)
    unified_sensor_json=PythonOperator(task_id='unified_sensor_json', python_callable=unified_sensor_json)
    unified_instance_json=PythonOperator(task_id='unified_instance_json', python_callable=unified_instance_json)
    unified_attribute_json=PythonOperator(task_id='unified_attribute_json', python_callable=unified_attribute_json)
    unified_frame_json=PythonOperator(task_id='unified_frame_json', python_callable=unified_frame_json)
    unified_log_json=PythonOperator(task_id='unified_log_json', python_callable=unified_log_json)
    unified_calibrated_sensor_json=PythonOperator(task_id='unified_calibrated_sensor_json', python_callable=unified_calibrated_sensor_json)
    unified_category_json=PythonOperator(task_id='unified_category_json', python_callable=unified_category_json)
    unified_ego_pose_json=PythonOperator(task_id='unified_ego_pose_json', python_callable=unified_ego_pose_json)
    unified_frame_data_json=PythonOperator(task_id='unified_frame_data_json', python_callable=unified_frame_data_json)
    unified_dataset_json=PythonOperator(task_id='unified_dataset_json', python_callable=unified_dataset_json)
    unified_map_json=PythonOperator(task_id='unified_map_json', python_callable=unified_map_json)
    unified_frame_annotation_json=PythonOperator(task_id='unified_frame_annotation_json', python_callable=unified_frame_annotation_json)


[unified_annotation_type_json, unified_visibility_json, unified_lidarseg_json, unified_sensor_json, unified_instance_json, unified_attribute_json, unified_frame_json, unified_log_json, unified_calibrated_sensor_json, unified_category_json, unified_ego_pose_json, unified_frame_data_json, unified_dataset_json, unified_map_json, unified_frame_annotation_json]