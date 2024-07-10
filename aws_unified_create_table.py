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


def unified_create_table():
    
    #mysql 접속 정보
    mysql_hook=MySqlHook(conn_id="aws_mysql")
    conn_info=mysql_hook.get_connection(conn_id="aws_mysql")
    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        client_flag=CLIENT.MULTI_STATEMENTS,
        autocommit=False
    )

    cur=conn.cursor()

    sql="""

    CREATE TABLE IF NOT EXISTS dataset (token char(32) PRIMARY KEY, log_token char(32) NOT NULL, base_path varchar(75), num_frames int NOT NULL, first_frame_token char(32) NOT NULL, last_frame_token char(32) NOT NULL);

    CREATE TABLE IF NOT EXISTS scenario (name varchar(10) PRIMARY KEY, description varchar(140));

    CREATE TABLE IF NOT EXISTS scenarios_in_dataset (dataset_token char(32), scenario_name varchar(10), PRIMARY KEY (dataset_token, scenario_name));

    CREATE TABLE IF NOT EXISTS log_map (log_token char(32), map_token char(32), PRIMARY KEY (log_token, map_token));

    CREATE TABLE IF NOT EXISTS frame (token char(32) PRIMARY KEY, timestamp varchar(16), dataset_token char(32), prev char(32), next char(32));

    CREATE TABLE IF NOT EXISTS ego_pose (token char(32) PRIMARY KEY, timestamp varchar(16), translation json, rotation json, coordinate_system json);

    CREATE TABLE IF NOT EXISTS log (token char(32) PRIMARY KEY, device varchar(26), date_captured varchar(19), location varchar(24), driving_distance decimal(17,13), device_wlh json, logfile char(29));

    CREATE TABLE IF NOT EXISTS visibility (token char(1) PRIMARY KEY, level varchar(7), description varchar(49));

    CREATE TABLE IF NOT EXISTS category (token char(32) PRIMARY KEY, name varchar(50), description varchar(320), default_scale json, `index` int);

    CREATE TABLE IF NOT EXISTS instance (token char(32) PRIMARY KEY, category_token char(32), first_frame_annotation_token char(32), last_frame_annotation_token char(32), num_annotations int);

    CREATE TABLE IF NOT EXISTS frame_data (token char(32) PRIMARY KEY, frame_token char(32), ego_pose_token char(32), calibrated_sensor_token char(32), timestamp varchar(16), is_key_frame boolean, height int, width int, file_name varchar(104), file_format char(3), prev char(32), next char(32));

    CREATE TABLE IF NOT EXISTS map (token char(32) PRIMARY KEY, category char(14), filename char(41));

    CREATE TABLE IF NOT EXISTS lidarseg (token char(32) PRIMARY KEY, frame_data_token char(32), filename char(64));

    CREATE TABLE IF NOT EXISTS annotation_type (name varchar(19) PRIMARY KEY, description varchar(34));

    CREATE TABLE IF NOT EXISTS sensor (token char(32) PRIMARY KEY, channel varchar(17), modality varchar(6));

    CREATE TABLE IF NOT EXISTS calibrated_sensor (token char(32) PRIMARY KEY, sensor_token char(32), translation json, rotation json, camera_intrinsic json, height_from_ground decimal(17,16));

    CREATE TABLE IF NOT EXISTS attribute (token char(32), name varchar(35), description varchar(100));

    CREATE TABLE IF NOT EXISTS frame_annotation (token char(32), frame_token char(32), frame_data_token char(32), instance_token char(32), visibility_token char(1), annotation_type_name varchar(19), is_lidar_synced boolean, size_wlh json, translation_center json, rotation_orientation json, bbox_image3d json, num_lidar_pts int, num_radar_pts int, attribute_tokens json, frame_aligned int, description varchar(50), prev char(32), next char(32));

    """

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()


with DAG('aws_unified_create_table',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    unified_create_table=PythonOperator(task_id='unified_create_table', python_callable=unified_create_table)


unified_create_table