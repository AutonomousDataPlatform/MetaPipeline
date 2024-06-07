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


def nuscenes_create_table():

    #mysql 접속 정보
    mysql_hook=MySqlHook(conn_id="eun_mysql")
    conn_info=mysql_hook.get_connection(conn_id="eun_mysql")
    conn=pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='nuScenes',
        port=conn_info.port,
        client_flag=CLIENT.MULTI_STATEMENTS,
        autocommit=False
    )
    cur=conn.cursor()

    sql="""

    CREATE TABLE IF NOT EXISTS attribute (token char(32) PRIMARY KEY, name varchar(35), description varchar(100));

    CREATE TABLE IF NOT EXISTS calibrated_sensor (token char(32) PRIMARY KEY, sensor_token char(32), translation_x decimal(17,16), translation_y decimal(17,16), translation_z decimal(17,16), rotation_w decimal(21,20), rotation_x decimal(21,20), rotation_y decimal(21,20), rotation_z decimal(21,20), intrinsic_parameter_fx decimal(19,15), intrinsic_parameter_skew1 decimal(2,1), intrinsic_parameter_cx decimal(19,15), intrinsic_parameter_skew2 decimal(2,1), intrinsic_parameter_fy decimal(19,15), intrinsic_parameter_cy decimal(19,15), intrinsic_parameter_projection_vector json);

    CREATE TABLE IF NOT EXISTS category (token char(32) PRIMARY KEY, name varchar(40), description varchar(320), `index` int);

    CREATE TABLE IF NOT EXISTS ego_pose (token char(32) PRIMARY KEY, rotation_w decimal(21,20), translation_x decimal(18,14), translation_y decimal(18,14), translation_z decimal(2,1), rotation_x decimal(21,20), rotation_y decimal(21,20), rotation_z decimal(21,20), timestamp char(16));

    CREATE TABLE IF NOT EXISTS instance (token char(32) PRIMARY KEY, category_token char(32), nbr_annotations int, first_annotation_token char(32), last_annotation_token char(32));

    CREATE TABLE IF NOT EXISTS log (token char(32) PRIMARY KEY, logfile char(29), vehicle char(4), date_captured date, location varchar(24));

    CREATE TABLE IF NOT EXISTS map (token char(32) PRIMARY KEY, category char(14), filename char(41), log_tokens json);

    CREATE TABLE IF NOT EXISTS sample (token char(32) PRIMARY KEY, timestamp char(16), prev char(32), next char(32), scene_token char(32));

    CREATE TABLE IF NOT EXISTS sample_annotation (token char(32) PRIMARY KEY, sample_token char(32), instance_token char(32), visibility_token char(1), attribute_tokens char(32), translation_center_x decimal(7,3), translation_center_y decimal(7,3), translation_center_z decimal(4,3), size_width decimal(5,3), size_length decimal(5,3), size_height decimal(5,3), rotation_w decimal(18,17), rotation_x decimal(2,1), rotation_y decimal(2,1), rotation_z decimal(18,17), prev char(32), next char(32), num_lidar_pts int, num_radar_pts int);

    CREATE TABLE IF NOT EXISTS sample_data (token char(32) PRIMARY KEY, sample_token char(32), ego_pose_token char(32), calibrated_sensor_token char(32), timestamp char(16), fileformat char(3), is_key_frame boolean, height int, width int, filename varchar(100), prev char(32), next char(32));

    CREATE TABLE IF NOT EXISTS scene (token char(32) PRIMARY KEY, log_token char(32), nbr_samples int, first_sample_token char(32), last_sample_token char(32), name char(10), description varchar(120));

    CREATE TABLE IF NOT EXISTS sensor (token char(32) PRIMARY KEY, channel varchar(17), modality varchar(6));

    CREATE TABLE IF NOT EXISTS visibility (token char(1) PRIMARY KEY, description varchar(49), level varchar(7));

    CREATE TABLE IF NOT EXISTS lidarseg (token char(32) PRIMARY KEY, sample_data_token char(32), filename char(64));

    """

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()


with DAG('nuscenes_create_table',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    nuscenes_create_table=PythonOperator(task_id='nuscenes_create_table', python_callable=nuscenes_create_table)


nuscenes_create_table