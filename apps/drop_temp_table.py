import pymysql
import pymysql.cursors
import dbinfo

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
        cursor = connection.cursor()

        tables = [
            "annotation_type_temp",
            "attribute_temp",
            "calibrated_sensor_temp",
            "category_temp",
            "dataset_temp",
            "scene_temp",
            "ego_pose_temp",
            "frame_data_temp",
            "frame_temp",
            "instance_temp",
            "lidarseg_temp",
            "log_temp",
            "map_temp",
            "sensor_temp",
            "visibility_temp",
            "frame_annotation_temp",
            "scenario_temp",
            "temp_filtered"     # 중간에 종료되면 temp_filtered 테이블 삭제 안되어있음
        ]

        for table in tables:
            try:
                query = f"DROP TABLE IF EXISTS {table}"
                cursor.execute(query)
                print(f"{table} table drop complete")
            except Exception as e:
                print(f"[WARNING] Failed to drop {table}: {e}")

        connection.commit()

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
