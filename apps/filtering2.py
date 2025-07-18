import pymysql
import boto3
import pymysql.cursors
import dbinfo

def replace_table_with_temp(cursor, table):
    query = "TRUNCATE TABLE " + table
    cursor.execute(query)
    query = "INSERT INTO " + table + " SELECT * FROM temp_filtered"
    cursor.execute(query)
    query = "DROP TABLE temp_filtered"
    cursor.execute(query)

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
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        cursor.execute("select count(*) from temp_filtered")
        result = cursor.fetchone()
        print(result)

        replace_table_with_temp(cursor, "frame_annotation_temp")

        # 최초 필터링된 frame 데이터를 저장하기 위해 frame_temp 테이블 생성
        cursor.execute("""
            CREATE TABLE frame_temp (
                `token` char(32) NOT NULL,
                `timestamp` varchar(16) DEFAULT NULL,
                `scene_token` char(32) DEFAULT NULL,
                `prev` char(32) DEFAULT NULL,
                `next` char(32) DEFAULT NULL,
                PRIMARY KEY (`token`)
            )
        """)

        # frame_data로 frame 필터링
        cursor.execute("""
            INSERT INTO frame_temp
            SELECT f.* FROM frame f WHERE f.token IN (SELECT fdt.frame_token FROM frame_data_temp fdt)
        """)
        print("frame_data 로 frame 필터링")

        # scene 으로 frame 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT ft.* FROM frame_temp ft WHERE ft.scene_token IN (SELECT st.token FROM scene_temp st)
        """)
        replace_table_with_temp(cursor, "frame_temp")
        print("scene 으로 frame 필터링")

        # frame_annotation 으로 frame 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT ft.* FROM frame_temp ft WHERE ft.token IN (SELECT fat.frame_token FROM frame_annotation_temp fat)
        """)
        replace_table_with_temp(cursor, "frame_temp")
        print("frame_annotation 으로 frame 필터링")

        # frame 으로 frame_annotation 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT fat.* from frame_annotation_temp fat 
            JOIN frame_temp ft ON fat.frame_token = ft.token
        """)
        replace_table_with_temp(cursor, "frame_annotation_temp")
        print("frame 으로 frame_annotation 필터링")

        # frame 으로 scene 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT * FROM scene_temp st WHERE st.token IN (SELECT ft.scene_token FROM frame_temp ft)
        """)
        replace_table_with_temp(cursor, "scene_temp")
        print("frame 으로 scene 필터링")

        # frame 으로 frame_data 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT fdt.* FROM frame_data_temp fdt WHERE fdt.frame_token IN (SELECT ft.token FROM frame_temp ft)
        """)
        replace_table_with_temp(cursor, "frame_data_temp")
        print("frame 으로 frame_data 필터링")

        # visibility_temp 테이블 생성 및 필터링
        cursor.execute("""
            CREATE TABLE visibility_temp (
                `token` char(1) NOT NULL,
                `level` varchar(7) DEFAULT NULL,
                `description` varchar(49) DEFAULT NULL,
                PRIMARY KEY (`token`)
            )
        """)
        cursor.execute("""
            INSERT INTO visibility_temp
            SELECT v.* FROM visibility v WHERE v.token IN (SELECT fat.visibility_token FROM frame_annotation_temp fat)
        """)
        print("frame_annotation 으로 visibility 필터링")

        # annotation_type_temp 테이블 생성 및 필터링
        cursor.execute("""
            CREATE TABLE annotation_type_temp (
                `name` varchar(19) NOT NULL,
                `description` varchar(34) DEFAULT NULL,
                PRIMARY KEY (`name`)
            )
        """)
        cursor.execute("""
            INSERT INTO annotation_type_temp
            SELECT at.* FROM annotation_type at WHERE at.name IN (SELECT fat.annotation_type_name FROM frame_annotation_temp fat)
        """)
        print("frame_annotation 으로 annotation_type 필터링")

        # frame_annotation 으로 attribute 필터링
        cursor.execute("""
            CREATE TABLE temp_filtered (
                `token` char(32) DEFAULT NULL,
                `name` varchar(35) DEFAULT NULL,
                `description` varchar(100) DEFAULT NULL
            )
        """)
        cursor.execute("""
            CREATE TEMPORARY TABLE afa_temp AS
            SELECT afa.attribute_token 
            FROM attribute_frame_annotation afa
            JOIN frame_annotation_temp fat ON afa.frame_annotation_token = fat.token
            GROUP BY afa.attribute_token
        """)
        cursor.execute("""
            INSERT INTO temp_filtered
            SELECT at.* FROM attribute_temp at
            JOIN afa_temp aft ON at.token = aft.attribute_token
        """)
        replace_table_with_temp(cursor, "attribute_temp")

        # frame_annotation 으로 instance 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT it.* FROM instance_temp it WHERE it.token IN (SELECT fat.instance_token FROM frame_annotation_temp fat)
        """)
        replace_table_with_temp(cursor, "instance_temp")
        print("frame_annotation 으로 instance 필터링")

        # instance 로 category 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT ct.* FROM category_temp ct WHERE ct.token IN (SELECT it.category_token FROM instance_temp it)
        """)
        replace_table_with_temp(cursor, "category_temp")
        print("instance 로 category 필터링")

        # scene 으로 log 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT lt.* FROM log_temp lt WHERE lt.token IN (SELECT st.log_token FROM scene_temp st)
        """)
        replace_table_with_temp(cursor, "log_temp")
        print("scene 으로 log 필터링")

        # log로 dataset 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT dt.* FROM dataset_temp dt WHERE dt.token IN (SELECT lt.dataset_token FROM log_temp lt)
        """)
        replace_table_with_temp(cursor, "dataset_temp")
        print("log로 dataset 필터링")

        # map_temp 생성 및 log 기준 필터링 => map 테이블 처음 필터링 되어서
        # log로 map 필터링
        cursor.execute("""
            CREATE TABLE map_temp (
                `token` char(32) NOT NULL,
                `category` char(14) DEFAULT NULL,
                `filename` char(41) DEFAULT NULL,
                PRIMARY KEY (`token`)
            )
        """)
        cursor.execute("""
            INSERT INTO map_temp
            SELECT m.* FROM map m WHERE m.token IN (
                SELECT lm.map_token FROM log_map lm WHERE lm.log_token IN (SELECT lt.token FROM log_temp lt)
            )
        """)
        print("log 로 map 필터링")

        # scene 으로 scenario 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT srt.* FROM scenario_temp srt WHERE srt.token IN (
                SELECT sis.scenario_token FROM scenarios_in_scene sis WHERE sis.scene_token IN (SELECT st.token FROM scene_temp st)
            )
        """)
        replace_table_with_temp(cursor, "scenario_temp")
        print("scene 으로 scenario 필터링")

        # ego_pose_temp 테이블 생성 및 frame_data 기준 필터링
        cursor.execute("""
            CREATE TABLE ego_pose_temp (
                `token` char(32) NOT NULL,
                `timestamp` varchar(16) DEFAULT NULL,
                `translation` json DEFAULT NULL,
                `rotation` json DEFAULT NULL,
                `coordinate_system` json DEFAULT NULL,
                `pose_type` varchar(7) DEFAULT NULL,
                `velocity` json DEFAULT NULL,
                `acceleration` json DEFAULT NULL,
                `angular_rate` json DEFAULT NULL,
                PRIMARY KEY (`token`)
            )
        """)
        cursor.execute("""
            INSERT INTO ego_pose_temp
            SELECT ep.* FROM ego_pose ep WHERE ep.token IN (SELECT fdt.ego_pose_token FROM frame_data_temp fdt)
        """)
        print("frame_data 로 ego_pose 필터링")

        # lidarseg_temp 테이블 생성 및 frame_data 기준 필터링
        cursor.execute("""
            CREATE TABLE lidarseg_temp (
                `token` char(32) NOT NULL,
                `frame_data_token` char(32) DEFAULT NULL,
                `filename` char(64) DEFAULT NULL,
                PRIMARY KEY (`token`)
            )
        """)
        cursor.execute("""
            INSERT INTO lidarseg_temp
            SELECT l.* FROM lidarseg l WHERE l.frame_data_token IN (SELECT fdt.token FROM frame_data_temp fdt)
        """)
        print("frame_data 로 lidarseg 필터링")

        # frame_data 로 calibrated_sensor 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT cst.* FROM calibrated_sensor_temp cst WHERE cst.token IN (SELECT fdt.calibrated_sensor_token FROM frame_data_temp fdt)
        """)
        replace_table_with_temp(cursor, "calibrated_sensor_temp")
        print("frame_data 로 calibrated_sensor 필터링")

        # calibrated_sensor 로 sensor 필터링
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_filtered AS
            SELECT st.* FROM sensor_temp st WHERE st.token IN (SELECT cst.sensor_token FROM calibrated_sensor_temp cst)
        """)
        replace_table_with_temp(cursor, "sensor_temp")
        print("calibrated_sensor 로 sensor 필터링")

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
