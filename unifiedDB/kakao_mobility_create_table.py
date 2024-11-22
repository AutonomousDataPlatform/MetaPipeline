import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymysql
from pymysql.constants import CLIENT

default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


def kakao_mobility_create_table():
    
    #mysql 접속 정보
    mysql_hook=MySqlHook(conn_id="eun_mysql")
    conn_info=mysql_hook.get_connection(conn_id="eun_mysql")
    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='kakao_mobility',
        port=conn_info.port,
        client_flag=CLIENT.MULTI_STATEMENTS,
        autocommit=False
    )

    cur=conn.cursor()

    sql="""

    CREATE TABLE IF NOT EXISTS log (uuid char(36) PRIMARY KEY, map_uuids char(36), device varchar(30) NOT NULL, date_captured datetime NOT NULL, location varchar(15) NOT NULL, driving_distance decimal(17,13) NOT NULL, device_width decimal(4,3), device_length decimal(4,3),device_height decimal(4,3));

    CREATE TABLE IF NOT EXISTS ego_pose (uuid char(36) PRIMARY KEY, timestamp varchar(20), translation_x decimal(25,20) NOT NULL, translation_y decimal(25,20) NOT NULL, translation_z decimal(25,20) NOT NULL, rotation_q0 decimal(21,20) NOT NULL, rotation_q1 decimal(21,20) NOT NULL, rotation_q2 decimal(21,20) NOT NULL, rotation_q3 decimal(21,20) NOT NULL, coordinate_system varchar(50));


    CREATE TABLE IF NOT EXISTS sensor (uuid char(36) PRIMARY KEY, name varchar(13) NOT NULL, type varchar(10) NOT NULL, translation_x decimal(21,20), translation_y decimal(21,20), translation_z decimal(21,20), rotation_q0 decimal(21,20), rotation_q1 decimal(21,20), rotation_q2 decimal(21,20), rotation_q3 decimal(21,20), intrinsic_model char(12), intrinsic_parameter_fx decimal(19,15), intrinsic_parameter_skew1 int, intrinsic_parameter_cx decimal(19,15), intrinsic_parameter_skew2 int, intrinsic_parameter_fy decimal(19,15), intrinsic_parameter_cy decimal(19,15), intrinsic_parameter_projection_vector json, height_from_ground decimal(17,16));

    CREATE TABLE IF NOT EXISTS visibility (level int PRIMARY KEY, description varchar(10));

    CREATE TABLE IF NOT EXISTS annotation_type (name varchar(15) PRIMARY KEY, description varchar(25));

    CREATE TABLE IF NOT EXISTS scenario (name varchar(15) PRIMARY KEY, description varchar(50));

    CREATE TABLE IF NOT EXISTS category (name varchar(25) PRIMARY KEY, path varchar(50), description varchar(130), default_scale_x decimal(4,2), default_scale_y decimal(4,2), default_scale_z decimal(4,2), attribute json);

    CREATE TABLE IF NOT EXISTS dataset (uuid char(36) PRIMARY KEY, log_uuid char(36) NOT NULL, name varchar(75) NOT NULL, description varchar(85), base_path varchar(60) NOT NULL, num_frames int NOT NULL, first_frame_uuid char(36) NOT NULL, last_frame_uuid char(36) NOT NULL);

    CREATE TABLE IF NOT EXISTS scenarios_in_dataset (dataset_uuid char(36), scenario_name varchar(15));

    CREATE TABLE IF NOT EXISTS frame (uuid char(36) PRIMARY KEY, timestamp varchar(15), dataset_uuid char(36) NOT NULL, prev char(36), next char(36));

    CREATE TABLE IF NOT EXISTS frame_data (uuid char(36) PRIMARY KEY, frame_uuid char(36) NOT NULL, ego_pose_uuid char(36) NOT NULL, sensor_uuid char(36) NOT NULL, timestamp varchar(15), is_key_frame boolean NOT NULL, height int, width int, file_name varchar(20) NOT NULL, file_format varchar(5) NOT NULL, prev char(36), next char(36), prev_key_frame_data char(36), next_key_frame_data char(36));

    CREATE TABLE IF NOT EXISTS instance (uuid char(36) PRIMARY KEY, category_name varchar(25) NOT NULL, first_frame_annotation_uuid char(36) NOT NULL, last_frame_annotation_uuid char(36) NOT NULL, num_annotations int NOT NULL, description varchar(50));

    CREATE TABLE IF NOT EXISTS frame_annotation (uuid char(36), frame_data_uuid char(36) NOT NULL, instance_uuid char(36) NOT NULL, visibility_level int , annotation_type_name varchar(15) NOT NULL, is_lidar_synced boolean, geometry json, num_pts int, attribute json, frame_aligned int NOT NULL, description varchar(50), prev char(36), next char(36));

    """

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()


with DAG('kakao_mobility_create_table',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_mobility_create_table=PythonOperator(task_id='kakao_mobility_create_table', python_callable=kakao_mobility_create_table)


kakao_mobility_create_table
