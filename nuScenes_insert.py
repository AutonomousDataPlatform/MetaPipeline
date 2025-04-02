import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import pymysql
import os


default_args={
        'owner':'eun',
        'start_date':dt.datetime(2024,6,3),
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


#sftp connection 을 이용하여 파일 다운로드
#attribute 테이블 데이터 삽입
def attribute_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='attribute.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/attribute.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for attribute_data in json_data:
                token=attribute_data['token']
                name=attribute_data['name']
                description=attribute_data['description']

                data.append((token, name, description))

            query="INSERT IGNORE INTO attribute (token, name, description) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#calibrated_sensor 테이블 데이터 삽입
def calibrated_sensor_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='calibrated_sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/calibrated_sensor.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for calibrated_sensor_data in json_data:
                token=calibrated_sensor_data['token']
                sensor_token=calibrated_sensor_data['sensor_token']
                translation_x=calibrated_sensor_data['translation'][0]
                translation_y=calibrated_sensor_data['translation'][1]
                translation_z=calibrated_sensor_data['translation'][2]
                rotation_w=calibrated_sensor_data['rotation'][0]
                rotation_x=calibrated_sensor_data['rotation'][1]
                rotation_y=calibrated_sensor_data['rotation'][2]
                rotation_z=calibrated_sensor_data['rotation'][3]
                
                if calibrated_sensor_data['camera_intrinsic']:
                    intrinsic_parameter_fx=calibrated_sensor_data['camera_intrinsic'][0][0]
                    intrinsic_parameter_skew1=calibrated_sensor_data['camera_intrinsic'][0][1]
                    intrinsic_parameter_cx=calibrated_sensor_data['camera_intrinsic'][0][2]
                    intrinsic_parameter_skew2=calibrated_sensor_data['camera_intrinsic'][1][0]
                    intrinsic_parameter_fy=calibrated_sensor_data['camera_intrinsic'][1][1]
                    intrinsic_parameter_cy=calibrated_sensor_data['camera_intrinsic'][1][2]
                    intrinsic_parameter_projection_vector=calibrated_sensor_data['camera_intrinsic'][2]

                    data.append((token, sensor_token, translation_x, translation_y, translation_z, rotation_w, rotation_x, rotation_y, rotation_z, intrinsic_parameter_fx, intrinsic_parameter_skew1, intrinsic_parameter_cx, intrinsic_parameter_skew2, intrinsic_parameter_fy, intrinsic_parameter_cy, json.dumps(intrinsic_parameter_projection_vector)))

                else:
                    intrinsic_parameter_fx=None
                    intrinsic_parameter_skew1=None
                    intrinsic_parameter_cx=None
                    intrinsic_parameter_skew2=None
                    intrinsic_parameter_fy=None
                    intrinsic_parameter_cy=None
                    intrinsic_parameter_projection_vector=None
                    
                    data.append((token, sensor_token, translation_x, translation_y, translation_z, rotation_w, rotation_x, rotation_y, rotation_z, intrinsic_parameter_fx, intrinsic_parameter_skew1, intrinsic_parameter_cx, intrinsic_parameter_skew2, intrinsic_parameter_fy, intrinsic_parameter_cy, intrinsic_parameter_projection_vector))
                
                

            query="INSERT IGNORE INTO calibrated_sensor (token, sensor_token, translation_x, translation_y, translation_z, rotation_w, rotation_x, rotation_y, rotation_z, intrinsic_parameter_fx, intrinsic_parameter_skew1, intrinsic_parameter_cx, intrinsic_parameter_skew2, intrinsic_parameter_fy, intrinsic_parameter_cy, intrinsic_parameter_projection_vector) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#ego_pose 테이블 데이터 삽입
def ego_pose_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='ego_pose.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/ego_pose.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for ego_pose_data in json_data:
                token=ego_pose_data['token']
                timestamp=ego_pose_data['timestamp']
                rotation_w=ego_pose_data['rotation'][0]
                rotation_x=ego_pose_data['rotation'][1]
                rotation_y=ego_pose_data['rotation'][2]
                rotation_z=ego_pose_data['rotation'][3]
                translation_x=ego_pose_data['translation'][0]
                translation_y=ego_pose_data['translation'][1]
                translation_z=ego_pose_data['translation'][2]
                
                data.append((token, timestamp, rotation_w, rotation_x, rotation_y, rotation_z, translation_x, translation_y, translation_z))

            query="INSERT IGNORE INTO ego_pose (token, timestamp, rotation_w, rotation_x, rotation_y, rotation_z, translation_x, translation_y, translation_z) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#instance 테이블 데이터 삽입
def instance_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='instance.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/instance.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for instance_data in json_data:
                token=instance_data['token']
                category_token=instance_data['category_token']
                nbr_annotations=instance_data['nbr_annotations']
                first_annotation_token=instance_data['first_annotation_token']
                last_annotation_token=instance_data['last_annotation_token']
                
                data.append((token, category_token, nbr_annotations, first_annotation_token, last_annotation_token))

            query="INSERT IGNORE INTO instance (token, category_token, nbr_annotations, first_annotation_token, last_annotation_token) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#category 테이블 데이터 삽입
def category_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes_lidarseg/v1.0-mini'
    local_path='category.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/category.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for category_data in json_data:
                token=category_data['token']
                name=category_data['name']
                description=category_data['description']
                index=category_data['index']
                
                data.append((token, name, description, index))

            query="INSERT IGNORE INTO category (token, name, description, `index`) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#lidarseg 테이블 데이터 삽입
def lidarseg_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes_lidarseg/v1.0-mini'
    local_path='lidarseg.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/lidarseg.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for lidarseg_data in json_data:
                token=lidarseg_data['token']
                sample_data_token=lidarseg_data['sample_data_token']
                filename=lidarseg_data['filename']
                
                data.append((token, sample_data_token, filename))

            query="INSERT IGNORE INTO lidarseg (token, sample_data_token, filename) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
            
    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#log 테이블 데이터 삽입
def log_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='log.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/log.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for log_data in json_data:
                token=log_data['token']
                logfile=log_data['logfile']
                vehicle=log_data['vehicle']
                date_captured=log_data['date_captured']
                location=log_data['location']
                
                data.append((token, logfile, vehicle, date_captured, location))

            query="INSERT IGNORE INTO log (token, logfile, vehicle, date_captured, location) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#map 테이블 데이터 삽입
def map_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='map.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/map.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for map_data in json_data:
                token=map_data['token']
                category=map_data['category']
                filename=map_data['filename']
                log_tokens=map_data['log_tokens']
                
                data.append((token, category, filename, json.dumps(log_tokens)))

            query="INSERT IGNORE INTO map (token, category, filename, log_tokens) VALUES (%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#sample_annotation 테이블 데이터 삽입
def sample_annotation_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample_annotation.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_annotation_data in json_data:
                token=sample_annotation_data['token']
                sample_token=sample_annotation_data['sample_token']
                instance_token=sample_annotation_data['instance_token']
                visibility_token=sample_annotation_data['visibility_token']

                if sample_annotation_data['attribute_tokens']:
                    attribute_tokens=sample_annotation_data['attribute_tokens'][0]
                else:
                    attribute_tokens=None
                
                translation_center_x=sample_annotation_data['translation'][0]
                translation_center_y=sample_annotation_data['translation'][1]
                translation_center_z=sample_annotation_data['translation'][2]
                size_width=sample_annotation_data['size'][0]
                size_length=sample_annotation_data['size'][1]
                size_height=sample_annotation_data['size'][2]
                rotation_w=sample_annotation_data['rotation'][0]
                rotation_x=sample_annotation_data['rotation'][1]
                rotation_y=sample_annotation_data['rotation'][2]
                rotation_z=sample_annotation_data['rotation'][3]
                prev=sample_annotation_data['prev']
                next=sample_annotation_data['next']
                num_lidar_pts=sample_annotation_data['num_lidar_pts']
                num_radar_pts=sample_annotation_data['num_radar_pts']
                
                data.append((token, sample_token, instance_token, visibility_token, attribute_tokens, translation_center_x, translation_center_y, translation_center_z, size_width, size_length, size_height, rotation_w, rotation_x, rotation_y,rotation_z, prev, next, num_lidar_pts,num_radar_pts))

            query="INSERT IGNORE INTO sample_annotation (token, sample_token, instance_token, visibility_token, attribute_tokens, translation_center_x, translation_center_y, translation_center_z, size_width, size_length, size_height, rotation_w, rotation_x, rotation_y,rotation_z, prev, next, num_lidar_pts,num_radar_pts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#sample_data 테이블 데이터 삽입
def sample_data_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample_data.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample_data.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_data_data in json_data:
                token=sample_data_data['token']
                sample_token=sample_data_data['sample_token']
                ego_pose_token=sample_data_data['ego_pose_token']
                calibrated_sensor_token=sample_data_data['calibrated_sensor_token']
                timestamp=sample_data_data['timestamp']
                fileformat=sample_data_data['fileformat']
                is_key_frame=sample_data_data['is_key_frame']
                height=sample_data_data['height']
                width=sample_data_data['width']
                filename=sample_data_data['filename']
                prev=sample_data_data['prev']
                next=sample_data_data['next']


                data.append((token, sample_token, ego_pose_token, calibrated_sensor_token, timestamp, fileformat, is_key_frame, height, width, filename, prev, next))

            query="INSERT IGNORE INTO sample_data (token, sample_token, ego_pose_token, calibrated_sensor_token, timestamp, fileformat, is_key_frame, height, width, filename, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#sample 테이블 데이터 삽입
def sample_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_data in json_data:
                token=sample_data['token']
                timestamp=sample_data['timestamp']
                prev=sample_data['prev']
                next=sample_data['next']
                scene_token=sample_data['scene_token']
                
                data.append((token, timestamp, prev, next, scene_token))

            query="INSERT IGNORE INTO sample (token, timestamp, prev, next, scene_token) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#sample 테이블 데이터 삽입
def sample_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sample.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sample.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sample_data in json_data:
                token=sample_data['token']
                timestamp=sample_data['timestamp']
                prev=sample_data['prev']
                next=sample_data['next']
                scene_token=sample_data['scene_token']
                
                data.append((token, timestamp, prev, next, scene_token))

            query="INSERT IGNORE INTO sample (token, timestamp, prev, next, scene_token) VALUES (%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#scene 테이블 데이터 삽입
def scene_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='scene.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/scene.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for scene_data in json_data:
                token=scene_data['token']
                log_token=scene_data['log_token']
                nbr_samples=scene_data['nbr_samples']
                first_sample_token=scene_data['first_sample_token']
                last_sample_token=scene_data['last_sample_token']
                name=scene_data['name']
                description=scene_data['description']
                
                data.append((token, log_token, nbr_samples, first_sample_token,last_sample_token, name, description))

            query="INSERT IGNORE INTO scene (token, log_token, nbr_samples, first_sample_token,last_sample_token, name, description) VALUES (%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#sensor 테이블 데이터 삽입
def sensor_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/sensor.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for sensor_data in json_data:
        
                token=sensor_data['token']
                channel=sensor_data['channel']
                modality=sensor_data['modality']

                data.append((token, channel, modality))

            query="INSERT IGNORE INTO sensor (token, channel, modality) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


#sftp connection 을 이용하여 파일 다운로드
#visibility 테이블 데이터 삽입
def visibility_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")

    remote_path='/data/nuScenes/v1.0-mini'
    local_path='visibility.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use nuScenes")

    with hook.get_conn() as sftp_client:
        file_path=remote_path + '/visibility.json'
        sftp_client.get(file_path,local_path)

        with open(local_path) as f:
            json_data=json.load(f)
            data=[]
            
            for visibility_data in json_data:
        
                token=visibility_data['token']
                description=visibility_data['description']
                level=visibility_data['level']

                data.append((token, description, level))

            query="INSERT IGNORE INTO visibility (token, description, level) VALUES (%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)


with DAG('nuScenes_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    attribute_insert=PythonOperator(task_id='attribute_insert', python_callable=attribute_insert)
    calibrated_sensor_insert=PythonOperator(task_id='calibrated_sensor_insert', python_callable=calibrated_sensor_insert)
    ego_pose_insert=PythonOperator(task_id='ego_pose_insert', python_callable=ego_pose_insert)
    instance_insert=PythonOperator(task_id='instance_insert', python_callable=instance_insert)
    category_insert=PythonOperator(task_id='category_insert', python_callable=category_insert)
    lidarseg_insert=PythonOperator(task_id='lidarseg_insert', python_callable=lidarseg_insert)
    log_insert=PythonOperator(task_id='log_insert', python_callable=log_insert)
    map_insert=PythonOperator(task_id='map_insert', python_callable=map_insert)
    sample_annotation_insert=PythonOperator(task_id='sample_annotation_insert', python_callable=sample_annotation_insert)
    sample_data_insert=PythonOperator(task_id='sample_data_insert', python_callable=sample_data_insert)
    sample_insert=PythonOperator(task_id='sample_insert', python_callable=sample_insert)
    scene_insert=PythonOperator(task_id='scene_insert', python_callable=scene_insert)
    sensor_insert=PythonOperator(task_id='sensor_insert', python_callable=sensor_insert)
    visibility_insert=PythonOperator(task_id='visibility_insert', python_callable=visibility_insert)



[attribute_insert, calibrated_sensor_insert, ego_pose_insert, instance_insert, category_insert, lidarseg_insert, log_insert, map_insert, sample_annotation_insert, sample_data_insert, sample_insert, scene_insert, sensor_insert, visibility_insert]
