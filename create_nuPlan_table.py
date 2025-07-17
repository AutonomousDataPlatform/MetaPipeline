## nuPlan 테이블 생성
## nuPlan은 JOIN 시 lidar_box와 track 테이블도 필요함.

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymysql
from pymysql.constants import CLIENT

default_args = {
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes = 5),
}


def create_table():
    
    #mysql 접속 정보
    mysql_hook = MySqlHook(conn_id = "aws_mysql")
    conn_info = mysql_hook.get_connection(conn_id = "aws_mysql")
    conn = pymysql.connect(
        host = conn_info.host,
        user = conn_info.login,
        password = conn_info.password,
        db = 'nuPlan',
        port = conn_info.port,
        client_flag = CLIENT.MULTI_STATEMENTS,
        autocommit = False
    )

    cur = conn.cursor()

    sql = """

    CREATE TABLE IF NOT EXISTS scene (
        token char(32) PRIMARY KEY NOT NULL, 
        log_token char(32), 
        base_path varchar(200), 
        num_frames int, 
        first_frame_token char(32), 
        last_frame_token char(32), 
        goal_ego_pose_token char(32),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS dataset (
        token CHAR(32) NOT NULL, 
        name VARCHAR(64),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS scenario (
        token char(32) PRIMARY KEY NOT NULL, 
        description varchar(140),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS scenarios_in_scene (
        scene_token char(32), 
        scenario_token char(32), 
        PRIMARY KEY (scene_token, scenario_token)
    );

    CREATE TABLE IF NOT EXISTS log_map (
        log_token char(32), 
        map_token char(32), 
        PRIMARY KEY (log_token, map_token)
    );

    CREATE TABLE IF NOT EXISTS frame (
        token char(32) PRIMARY KEY NOT NULL, 
        timestamp varchar(16), 
        scene_token char(32), 
        prev char(32), 
        next char(32),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS ego_pose (
        token char(32) PRIMARY KEY NOT NULL, 
        timestamp varchar(16), 
        translation json, 
        rotation json, 
        coordinate_system json, 
        pose_type varchar(7), 
        velocity json, acceleration json, angular_rate json,
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS visibility (
        token char(1) PRIMARY KEY, 
        level varchar(7), 
        description varchar(49),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS category (
        token char(32) PRIMARY KEY NOT NULL, 
        name varchar(50), 
        description varchar(320), 
        default_scale json, 
        `index` int,
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS instance (
        token char(32) PRIMARY KEY NOT NULL, 
        category_token char(32), 
        first_frame_annotation_token char(32), 
        last_frame_annotation_token char(32), 
        num_annotations int, 
        size json,
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS frame_data (
        token char(32) PRIMARY KEY NOT NULL, 
        frame_token char(32), 
        ego_pose_token char(32), 
        calibrated_sensor_token char(32), 
        timestamp varchar(16), 
        is_key_frame boolean, 
        height int, 
        width int, 
        file_name varchar(104), 
        file_format char(3), 
        prev char(32), 
        next char(32),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS map (
        token char(32) PRIMARY KEY, 
        category char(14), 
        filename char(41),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS lidarseg (
        token char(32) PRIMARY KEY, 
        frame_data_token char(32), 
        filename char(64),
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS annotation_type (
        name varchar(19) PRIMARY KEY, 
        description varchar(34)
    );

    CREATE TABLE IF NOT EXISTS sensor (
        token char(32) PRIMARY KEY NOT NULL, 
        channel varchar(17), 
        modality varchar(6), 
        PRIMARY KEY (token)
    );

    CREATE TABLE IF NOT EXISTS calibrated_sensor (
        token char(32) PRIMARY KEY NOT NULL, 
        sensor_token char(32), 
        translation json, 
        rotation json, 
        camera_intrinsic json, 
        height_from_ground decimal(17,16), 
        distortion json
    );

    CREATE TABLE IF NOT EXISTS attribute (
        token char(32) NOT NULL, 
        name varchar(35) UNIQUE, 
        description varchar(100)
    );

    CREATE TABLE IF NOT EXISTS frame_annotation (
        token char(32) NOT NULL, 
        frame_token char(32), 
        frame_data_token char(32), 
        instance_token char(32), 
        visibility_token char(1), 
        annotation_type_name varchar(19), 
        is_lidar_synced boolean, 
        size_wlh json, 
        translation_center json, 
        rotation_orientation json, 
        bbox_image3d json, 
        bbox_image2d json, 
        num_lidar_pts int, 
        num_radar_pts int, 
        attribute_tokens json, 
        frame_aligned int, 
        description varchar(50), 
        prev char(32), 
        next char(32), 
        velocity json
    );

    CREATE TABLE IF NOT EXISTS attribute_frame_annotation (
        `attribute_token` char(32) NOT NULL, 
        `frame_annotation_token` char(32) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS lidar_box (
        token char(32),
        lidar_pc_token char(32),
        track_token char(32), 
        next_token char(32),
        prev_token char(32),
        translation_center json,
        size_wlh json,
        velocity json,
        yaw json
    );

    CREATE TABLE IF NOT EXISTS track (
        token char(32) PRIMARY KEY NOT NULL, 
        category_token char(32), 
        size json
    );

    """

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()


with DAG('create_nuPlan_table',
        default_args = default_args,
        schedule_interval = None,
        )as dag:
    create_table_task = PythonOperator(task_id = 'create_table', python_callable = create_table)
