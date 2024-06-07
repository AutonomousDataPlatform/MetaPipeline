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


remote_path='/data/3D_object_detection_tracking(autonomous_driving)'



#sensor 테이블 데이터 삽입 함수
#sftp connection 을 이용하여 파일 다운로드
def sensor_insert():

    hook=SFTPHook(ssh_conn_id="eun_sftp")
    directory_list=hook.list_directory(remote_path)
    #local 에 저장할 때 파일 이름
    local_path='sensor.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use kakao_mobility")

    with hook.get_conn() as sftp_client:
        for dir_name in directory_list:
            file_path=remote_path + '/' + dir_name + '/meta/sensor.json'
            sftp_client.get(file_path, local_path)

            with open(local_path) as f:
                json_data=json.load(f)

                data=[]

                for sensor_data in json_data:

                    uuid=sensor_data['uuid']
                    name=sensor_data['name']
                    type=sensor_data['type']
                    translation_x=sensor_data['translation'][0]
                    translation_y=sensor_data['translation'][1]
                    translation_z=sensor_data['translation'][2]
                    rotation_q0=sensor_data['rotation'][0]
                    rotation_q1=sensor_data['rotation'][1]
                    rotation_q2=sensor_data['rotation'][2]
                    rotation_q3=sensor_data['rotation'][3]
                    height_from_ground=sensor_data['height_from_ground']

                    if sensor_data['intrinsic']['model'] == None:
                    
                        uuid=sensor_data['uuid']
                        intrinsic_model=None
                        intrinsic_parameter_fx=None
                        intrinsic_parameter_skew1=None
                        intrinsic_parameter_cx=None
                        intrinsic_parameter_skew2=None
                        intrinsic_parameter_fy=None
                        intrinsic_parameter_cy=None
                        intrinsic_parameter_projection_vector=None

                        data.append((uuid,name,type,translation_x,translation_y,translation_z,rotation_q0,rotation_q1,rotation_q2,rotation_q3,intrinsic_model,intrinsic_parameter_fx,intrinsic_parameter_skew1,intrinsic_parameter_cx,intrinsic_parameter_skew2,intrinsic_parameter_fy,intrinsic_parameter_cy,intrinsic_parameter_projection_vector,height_from_ground))

                
                    else:
                        uuid=sensor_data['uuid']
                        intrinsic_model=sensor_data['intrinsic']['model']
                        intrinsic_parameter_fx=sensor_data['intrinsic']['parameter'][0][0]
                        intrinsic_parameter_skew1=sensor_data['intrinsic']['parameter'][0][1]
                        intrinsic_parameter_cx=sensor_data['intrinsic']['parameter'][0][2]
                        intrinsic_parameter_skew2=sensor_data['intrinsic']['parameter'][1][0]
                        intrinsic_parameter_fy=sensor_data['intrinsic']['parameter'][1][1]
                        intrinsic_parameter_cy=sensor_data['intrinsic']['parameter'][1][2]
                        intrinsic_parameter_projection_vector=sensor_data['intrinsic']['parameter'][2]

                        data.append((uuid,name,type,translation_x,translation_y,translation_z,rotation_q0,rotation_q1,rotation_q2,rotation_q3,intrinsic_model,intrinsic_parameter_fx,intrinsic_parameter_skew1,intrinsic_parameter_cx,intrinsic_parameter_skew2,intrinsic_parameter_fy,intrinsic_parameter_cy,json.dumps(intrinsic_parameter_projection_vector),height_from_ground))

                query="INSERT IGNORE INTO sensor (uuid, name, type, translation_x, translation_y, translation_z, rotation_q0, rotation_q1, rotation_q2, rotation_q3, intrinsic_model, intrinsic_parameter_fx, intrinsic_parameter_skew1, intrinsic_parameter_cx, intrinsic_parameter_skew2, intrinsic_parameter_fy, intrinsic_parameter_cy, intrinsic_parameter_projection_vector, height_from_ground) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.executemany(query,data)
                conn.commit()

    cur.close()
    conn.close()

    os.remove(local_path)



with DAG('kakao_mobility_sensor_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    sensor_insert=PythonOperator(task_id='sensor_insert', python_callable=sensor_insert)


sensor_insert