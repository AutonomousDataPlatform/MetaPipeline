import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import os


default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


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


with DAG('nuScenes_calibrated_sensor_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    calibrated_sensor_insert=PythonOperator(task_id='calibrated_sensor_insert', python_callable=calibrated_sensor_insert)



calibrated_sensor_insert