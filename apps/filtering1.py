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

        # input 으로 들어온 condition
        dataset_token = event.get('dataset_token')
        category_token = event.get('category_token')
        scenario_token = event.get('scenario_token')
        log_token = event.get('log_token')
        attribute_token = event.get('attribute_token')
        file_format = event.get('file_format')
        annotation_type_name = event.get('annotation_type_name')
        sensor_token = event.get('sensor_token')

        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 1차 필터링 (condition 으로 들어올 수 있는 테이블 필터링)

        # dataset 1차 필터링
        create_query = """
        CREATE TABLE dataset_temp (
        `token` CHAR(32) NOT NULL, 
        `name` VARCHAR(64)
        )
        """
        cursor.execute(create_query)

        if dataset_token:   # dataset_token 이 있는 경우
            for i in range(len(dataset_token)):
                query = "INSERT INTO dataset_temp SELECT * FROM dataset WHERE token = %s"
                cursor.execute(query, dataset_token[i])

        # dataset이 condition이 아닌 경우
        else:
            query = "INSERT INTO dataset_temp SELECT * FROM dataset"
            cursor.execute(query)

        print("dataset 1차 필터링 end")


        # category 1차 필터링
        create_query = """
        CREATE TABLE category_temp (
        `token` char(32) NOT NULL,
        `name` varchar(50) DEFAULT NULL,
        `description` varchar(320) DEFAULT NULL,
        `default_scale` json DEFAULT NULL,
        `index` int DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(create_query)

        if category_token:
            for i in range(len(category_token)):
                query = "INSERT INTO category_temp SELECT * FROM category WHERE token = %s"
                cursor.execute(query, category_token[i])

        # category 가 condition 이 아닌 경우
        else:
            query = "INSERT INTO category_temp SELECT * FROM category"
            cursor.execute(query)
        
        print("category 1차 필터링 end")

        # scenario 1차 필터링
        create_query = """
        CREATE TABLE scenario_temp (
        token char(32) NOT NULL,
        `description` varchar(140) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(create_query)

        if scenario_token:
            for i in range(len(scenario_token)):
                query = "INSERT INTO scenario_temp SELECT * FROM scenario WHERE token = %s"
                cursor.execute(query, scenario_token[i])

        else:
            query = "INSERT INTO scenario_temp SELECT * FROM scenario"
            cursor.execute(query)
        print("scenario 1차 필터링 end")

        # log 1차 필터링
        create_query = """
        CREATE TABLE log_temp (
        `token` char(32) NOT NULL,
        `dataset_token` char(32) DEFAULT NULL,
        `device` varchar(26) DEFAULT NULL,
        `date_captured` varchar(19) DEFAULT NULL,
        `location` varchar(24) DEFAULT NULL,
        `driving_distance` decimal(17,13) DEFAULT NULL,
        `device_wlh` json DEFAULT NULL,
        `logfile` char(29) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """

        cursor.execute(create_query)

        if log_token:
            for i in range(len(log_token)):
                query = "INSERT INTO log_temp SELECT * FROM log WHERE token = %s"
                cursor.execute(query, log_token[i])

        else:
            query = "INSERT INTO log_temp SELECT * FROM log"
            cursor.execute(query)
        print("log 1차 필터링 end")

        # attribute 1차 필터링
        create_query = """
        CREATE TABLE attribute_temp (
        `token` char(32) DEFAULT NULL,
        `name` varchar(35) DEFAULT NULL,
        `description` varchar(100) DEFAULT NULL
        )
        """
        cursor.execute(create_query)

        if attribute_token:
            for i in range(len(attribute_token)):
                query = "INSERT INTO attribute_temp SELECT * FROM attribute WHERE token = %s"
                cursor.execute(query, attribute_token[i])

        else:
            query = "INSERT INTO attribute_temp SELECT * FROM attribute"
            cursor.execute(query)
        print("attribute 1차 필터링 end")

        # frame_data 1차 필터링
        create_query = """
        CREATE TABLE frame_data_temp (
        `token` char(32) NOT NULL,
        `frame_token` char(32) DEFAULT NULL,
        `ego_pose_token` char(32) DEFAULT NULL,
        `calibrated_sensor_token` char(32) DEFAULT NULL,
        `timestamp` varchar(16) DEFAULT NULL,
        `is_key_frame` tinyint(1) DEFAULT NULL,
        `height` int DEFAULT NULL,
        `width` int DEFAULT NULL,
        `file_name` varchar(104) DEFAULT NULL,
        `file_format` char(3) DEFAULT NULL,
        `prev` char(32) DEFAULT NULL,
        `next` char(32) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(create_query)

        if file_format:
            for i in range(len(file_format)):
                query = "INSERT INTO frame_data_temp SELECT * FROM frame_data WHERE file_format = %s"
                cursor.execute(query, file_format[i])

        else:
            query = "INSERT INTO frame_data_temp SELECT * FROM frame_data"
            cursor.execute(query)
        print("frame_data 1차 필터링 end")

        # frame_annotation 1차 필터링
        create_query = """
        CREATE TABLE frame_annotation_temp (
        `token` char(32) DEFAULT NULL,
        `frame_token` char(32) DEFAULT NULL,
        `frame_data_token` char(32) DEFAULT NULL,
        `instance_token` char(32) DEFAULT NULL,
        `visibility_token` char(1) DEFAULT NULL,
        `annotation_type_name` varchar(19) DEFAULT NULL,
        `is_lidar_synced` tinyint(1) DEFAULT NULL,
        `size_wlh` json DEFAULT NULL,
        `translation_center` json DEFAULT NULL,
        `rotation_orientation` json DEFAULT NULL,
        `bbox_image3d` json DEFAULT NULL,
        `bbox_image2d` json DEFAULT NULL,
        `num_lidar_pts` int DEFAULT NULL,
        `num_radar_pts` int DEFAULT NULL,
        `attribute_tokens` json DEFAULT NULL,
        `frame_aligned` int DEFAULT NULL,
        `description` varchar(50) DEFAULT NULL,
        `prev` char(32) DEFAULT NULL,
        `next` char(32) DEFAULT NULL,
        `velocity` json DEFAULT NULL
        )
        """
        cursor.execute(create_query)

        if annotation_type_name:
            for i in range(len(annotation_type_name)):
                query = "INSERT INTO frame_annotation_temp SELECT * FROM frame_annotation WHERE token = %s"
                cursor.execute(query, annotation_type_name[i])

        else:
            query = "INSERT INTO frame_annotation_temp SELECT * FROM frame_annotation"
            cursor.execute(query)
        print("frame_annotation 1차 필터링 end")

        # sensor 1차 필터링
        create_query = """
        CREATE TABLE sensor_temp (
        `token` char(32) NOT NULL,
        `channel` varchar(17) DEFAULT NULL,
        `modality` varchar(6) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(create_query)

        if sensor_token:
            for i in range(len(sensor_token)):
                query = "INSERT INTO sensor_temp SELECT * FROM sensor WHERE token = %s"
                cursor.execute(query, sensor_token[i])

        else:
            query = "INSERT INTO sensor_temp SELECT * FROM sensor"
            cursor.execute(query)
        print("sensor 1차 필터링 end")

        # 조건 없이 가져오기

        # 2.모든 조건 충족하는 frame 필터링
        query = """
        CREATE TABLE scene_temp (
        `token` char(32) NOT NULL,
        `type` varchar(14) DEFAULT NULL,
        `log_token` char(32) DEFAULT NULL,
        `base_path` varchar(200) DEFAULT NULL,
        `num_frames` int DEFAULT NULL,
        `first_frame_token` char(32) DEFAULT NULL,
        `last_frame_token` char(32) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(query)

        #log 로 scene 필터링
        query = "INSERT INTO scene_temp (SELECT * FROM scene s WHERE s.log_token IN (SELECT lt.token FROM log_temp lt))"
        cursor.execute(query)
        print("log 로 scene 필터링 end")

        # scenario 로 scene 필터링
        # 여기서는 scene_temp 를 이용하는 이유는 log 로 scene 이 한 번 필터링이 됐고, 필터링이 된 테이블을 scenario 로 다시 필터링 해야 하기 때문
        # 즉 필터링이 안 된 테이블인 경우는 원본 테이블을 이용하고, 1번 이상 필터링이 된 테이블인 경우는 _temp 테이블 이용
        query = """
        CREATE TEMPORARY TABLE temp_filtered AS
        SELECT st.* FROM scene_temp st 
        join (select sis.* from scenarios_in_scene sis WHERE sis.scenario_token in (SELECT srt.token from scenario_temp srt)) as sis
        on st.token = sis.scene_token
        """
        cursor.execute(query)

        replace_table_with_temp(cursor, "scene_temp")
        print("scenario 로 scene 필터링 end")

        # sensor 로 calibrated_sensor 필터링
        # 최초 필터링된 calibrated_sensor 데이터를 저장하기 위해 calibrated_sensor_temp 테이블 생성
        query = """
        CREATE TABLE calibrated_sensor_temp (
        `token` char(32) NOT NULL,
        `sensor_token` char(32) DEFAULT NULL,
        `translation` json DEFAULT NULL,
        `rotation` json DEFAULT NULL,
        `camera_intrinsic` json DEFAULT NULL,
        `height_from_ground` decimal(17,16) DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(query)
        query = """
        INSERT INTO calibrated_sensor_temp
        SELECT cs.* FROM calibrated_sensor cs
        JOIN sensor_temp st
        ON cs.sensor_token = st.token
        """
        cursor.execute(query)
        print("sensor 로 calibrated_sensor 필터링")

        # calibrated_sensor 로 frame_data 필터링
        query = """
        CREATE TEMPORARY TABLE temp_filtered AS
        SELECT fdt.* FROM frame_data_temp fdt 
        JOIN calibrated_sensor_temp cst
        ON fdt.calibrated_sensor_token = cst.token
        """
        cursor.execute(query)

        replace_table_with_temp(cursor, "frame_data_temp")
        print("calibrated_sensor 로 frame_data 필터링")

        # category 로 instance 필터링
        # 최초 필터링된 instance 데이터를 저장하기 위해 instance 테이블 생성
        query = """
        CREATE TABLE instance_temp (
        `token` char(32) NOT NULL,
        `category_token` char(32) DEFAULT NULL,
        `first_frame_annotation_token` char(32) DEFAULT NULL,
        `last_frame_annotation_token` char(32) DEFAULT NULL,
        `num_annotations` int DEFAULT NULL,
        `size` json DEFAULT NULL,
        PRIMARY KEY (`token`)
        )
        """
        cursor.execute(query)
        query = """
        INSERT INTO instance_temp
        SELECT i.* FROM `instance` i 
        JOIN category_temp ct 
        ON i.category_token = ct.token
        """
        cursor.execute(query)
        print("category 로 instance 필터링")

        # instance 로 frame_annotation 필터링
        query = """
        CREATE TEMPORARY TABLE temp_filtered AS
        SELECT fat.* FROM frame_annotation_temp fat 
        JOIN instance_temp it                           
        ON fat.instance_token = it.token
        """
        cursor.execute(query)

        replace_table_with_temp(cursor, "frame_annotation_temp")
        print("instance 로 frame_annotation 필터링")


        # attribute 로 frame_annotation 필터링 batch 처리

        query = """
        CREATE TABLE temp_filtered (
        `token` char(32) DEFAULT NULL,
        `frame_token` char(32) DEFAULT NULL,
        `frame_data_token` char(32) DEFAULT NULL,
        `instance_token` char(32) DEFAULT NULL,
        `visibility_token` char(1) DEFAULT NULL,
        `annotation_type_name` varchar(19) DEFAULT NULL,
        `is_lidar_synced` tinyint(1) DEFAULT NULL,
        `size_wlh` json DEFAULT NULL,
        `translation_center` json DEFAULT NULL,
        `rotation_orientation` json DEFAULT NULL,
        `bbox_image3d` json DEFAULT NULL,
        `bbox_image2d` json DEFAULT NULL,
        `num_lidar_pts` int DEFAULT NULL,
        `num_radar_pts` int DEFAULT NULL,
        `attribute_tokens` json DEFAULT NULL,
        `frame_aligned` int DEFAULT NULL,
        `description` varchar(50) DEFAULT NULL,
        `prev` char(32) DEFAULT NULL,
        `next` char(32) DEFAULT NULL,
        `velocity` json DEFAULT NULL
        )
        """
        cursor.execute(query)

        query = "SELECT COUNT(*) AS cnt FROM attribute_frame_annotation"
        cursor.execute(query)
        result = cursor.fetchone()
        total_batch = (result['cnt'] + 999999) // 1000000
        output = {'batch_number': [i for i in range(total_batch)]}

        connection.commit()
        return output

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