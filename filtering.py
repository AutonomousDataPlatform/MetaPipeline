import pymysql
import boto3
import pymysql.cursors
import dbinfo

connection = pymysql.connect(host = dbinfo.db_host, port = 23306, user = dbinfo.db_username, passwd = dbinfo.db_password, db = dbinfo.db_name)

def replace_table_with_temp(cursor, table):
    query = "TRUNCATE TABLE " + table
    cursor.execute(query)
    query = "INSERT INTO " + table + " SELECT * FROM temp_filtered"
    cursor.execute(query)
    query = "DROP TEMPORARY TABLE temp_filtered"
    cursor.execute(query)

def lambda_handler(event, context):

    # input 으로 들어온 condition
    category_token = event.get('category_token')
    scenario_name = event.get('scenario_name')
    log_token = event.get('log_token')
    attribute_token = event.get('attribute_token')
    file_format = event.get('file_format')
    annotation_type_name = event.get('annotation_type_name')
    sensor_token = event.get('sensor_token')

    cursor = connection.cursor(pymysql.cursors.DictCursor)

    # 1차 필터링 (condition 으로 들어올 수 있는 테이블 필터링)

    # category 1차 필터링
    create_query = """
    CREATE TABLE category_temp (
    `token` char(32) NOT NULL,
    `name` varchar(50) DEFAULT NULL,
    `description` varchar(320) DEFAULT NULL,
    `default_scale` json DEFAULT NULL,
    `index` int DEFAULT NULL,
    PRIMARY KEY (`token`)
    ) """
    cursor.execute(create_query)

    if category_token:
        for i in range(len(category_token)):
            if i == 0:
                query = "INSERT INTO category_temp SELECT * FROM category WHERE token = %s"
                cursor.execute(query, category_token[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM category_temp WHERE token = %s
                    """
                cursor.execute(query, category_token[i])

                replace_table_with_temp(cursor, "category_temp")

    # category 가 condition 이 아닌 경우
    else:
        query = "INSERT INTO category_temp SELECT * FROM category"
        cursor.execute(query)

    # scenario 1차 필터링
    create_query = """
    CREATE TABLE scenario_temp (
    `name` varchar(10) NOT NULL,
    `description` varchar(140) DEFAULT NULL,
    PRIMARY KEY (`name`)
    )
    """
    cursor.execute(create_query)

    if scenario_name:
        for i in range(len(scenario_name)):
            if i == 0:
                query = "INSERT INTO scenario_temp SELECT * FROM scenario WHERE name = %s"
                cursor.execute(query, scenario_name[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM scenario_temp WHERE token = %s
                    """
                cursor.execute(query, scenario_name[i])

                replace_table_with_temp(cursor, "scenario_temp")
    else:
        query = "INSERT INTO scenario_temp SELECT * FROM scenario"
        cursor.execute(query)

    # log 1차 필터링
    create_query = """
    CREATE TABLE log_temp (
    `token` char(32) NOT NULL,
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
            if i == 0:
                query = "INSERT INTO log_temp SELECT * FROM log WHERE token = %s"
                cursor.execute(query, log_token[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM log_temp WHERE token = %s
                    """
                cursor.execute(query, log_token[i])

                replace_table_with_temp(cursor, "log_temp")
    else:
        query = "INSERT INTO log_temp SELECT * FROM log"
        cursor.execute(query)

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
            if i == 0:
                query = "INSERT INTO attribute_temp SELECT * FROM attribute WHERE token = %s"
                cursor.execute(query, attribute_token[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM attribute_temp WHERE token = %s
                    """
                cursor.execute(query, attribute_token[i])

                replace_table_with_temp(cursor, "attribute_temp")

    else:
        query = "INSERT INTO attribute_temp SELECT * FROM attribute"
        cursor.execute(query)
    
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
            if i == 0:
                query = "INSERT INTO frame_data_temp SELECT * FROM frame_data WHERE file_format = %s"
                cursor.execute(query, file_format[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM frame_data_temp WHERE file_format = %s
                    """
                cursor.execute(query, file_format[i])

                replace_table_with_temp(cursor, "frame_data_temp")

    else:
        query = "INSERT INTO frame_data_temp SELECT * FROM frame_data"
        cursor.execute(query)

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
    `num_lidar_pts` int DEFAULT NULL,
    `num_radar_pts` int DEFAULT NULL,
    `attribute_tokens` json DEFAULT NULL,
    `frame_aligned` int DEFAULT NULL,
    `description` varchar(50) DEFAULT NULL,
    `prev` char(32) DEFAULT NULL,
    `next` char(32) DEFAULT NULL
    )
    """
    cursor.execute(create_query)

    if annotation_type_name:
        for i in range(len(annotation_type_name)):
            if i == 0:
                query = "INSERT INTO frame_annotation_temp SELECT * FROM frame_annotation WHERE token = %s"
                cursor.execute(query, annotation_type_name[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM frame_annotation_temp WHERE annotation_type_name = %s
                    """
                cursor.execute(query, annotation_type_name[i])

                replace_table_with_temp(cursor, "frame_annotation_temp")

    else:
        query = "INSERT INTO frame_annotation_temp SELECT * FROM frame_annotation"
        cursor.execute(query)

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
            if i == 0:
                query = "INSERT INTO sensor_temp SELECT * FROM sensor WHERE token = %s"
                cursor.execute(query, sensor_token[i])
            else:
                query = """
                    CREATE TEMPORARY TABLE temp_filtered AS
                    SELECT * FROM sensor_temp WHERE token = %s
                    """
                cursor.execute(query, sensor_token[i])

                replace_table_with_temp(cursor, "sensor_temp")

    else:
        query = "INSERT INTO sensor_temp SELECT * FROM sensor"
        cursor.execute(query)

    # 조건 없이 가져오기

    # 2.모든 조건 충족하는 frame 필터링
    query = """
    CREATE TABLE dataset_temp (
    `token` char(32) NOT NULL,
    `type` varchar(14) DEFAULT NULL,
    `log_token` char(32) NOT NULL,
    `base_path` varchar(75) DEFAULT NULL,
    `num_frames` int NOT NULL,
    `first_frame_token` char(32) NOT NULL,
    `last_frame_token` char(32) NOT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    #log 로 dataset 필터링
    query = "INSERT INTO dataset_temp (SELECT * FROM dataset d WHERE d.log_token IN (SELECT lt.token FROM log_temp lt))"
    cursor.execute(query)

    # scenario 로 dataset 필터링
    # 여기서는 dataset_temp 를 이용하는 이유는 log 로 dataset 이 한 번 필터링이 됐고, 필터링이 된 테이블을 scenario 로 다시 필터링 해야 하기 때문
    # 즉 필터링이 안 된 테이블인 경우는 원본 테이블을 이용하고, 1번 이상 필터링이 된 테이블인 경우는 _temp 테이블 이용
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT dt.* FROM dataset_temp dt 
    join (select sid.* from scenarios_in_dataset sid WHERE sid.scenario_name in (SELECT st.name from scenario_temp st)) as sid
    on dt.token = sid.dataset_token
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "dataset_temp")

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

    # calibrated_sensor 로 frame_data 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fdt.* FROM frame_data_temp fdt 
    JOIN calibrated_sensor_temp cst
    ON fdt.calibrated_sensor_token = cst.token
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_data_temp")

    # category 로 instance 필터링
    # 최초 필터링된 instance 데이터를 저장하기 위해 instance 테이블 생성
    query = """
    CREATE TABLE instance_temp (
    `token` char(32) NOT NULL,
    `category_token` char(32) DEFAULT NULL,
    `first_frame_annotation_token` char(32) DEFAULT NULL,
    `last_frame_annotation_token` char(32) DEFAULT NULL,
    `num_annotations` int DEFAULT NULL,
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

    # instance 로 frame_annotation 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fat.* FROM frame_annotation_temp fat 
    JOIN instance_temp it                           
    ON fat.instance_token = it.token
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_annotation_temp")

    # attribute 로 frame_annotation 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fat.* FROM frame_annotation_temp fat WHERE fat.token IN
    (SELECT afa.frame_annotation_token FROM attribute_frame_annotation afa WHERE afa.attribute_token IN 
    (SELECT at.token FROM `attribute_temp` at))
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_annotation_temp")

    # 최초 필터링된 frame 데이터를 저장하기 위해 frame_temp 테이블 생성
    query = """
    CREATE TABLE frame_temp (
    `token` char(32) NOT NULL,
    `timestamp` varchar(16) DEFAULT NULL,
    `dataset_token` char(32) DEFAULT NULL,
    `prev` char(32) DEFAULT NULL,
    `next` char(32) DEFAULT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    # frame_data 로 frame 필터링
    query = """
    INSERT INTO frame_temp
    SELECT f.* FROM frame f WHERE f.token IN (SELECT fdt.frame_token FROM frame_data_temp fdt)
    """
    cursor.execute(query)

    # dataset 으로 frame 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ft.* FROM frame_temp ft WHERE ft.dataset_token IN (SELECT dt.token FROM dataset_temp dt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_temp")

    # frame_annotaton 으로 frame 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ft.* FROM frame_temp ft WHERE ft.token IN (SELECT fat.frame_token FROM frame_annotation_temp fat)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_temp")

    # 3.frame 으로 나머지 테이블 필터링 (frame 외 모든 테이블 조건에 해당하는 데이터만 필터링)

    # frame 으로 frame_annotation 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fat.* from frame_annotation_temp fat 
    JOIN frame_temp ft 
    ON fat.frame_token = ft.token
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_annotation_temp")

    # frame 으로 dataset 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT * FROM dataset_temp dt WHERE dt.token IN (SELECT ft.dataset_token FROM frame_temp ft)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "dataset_temp")

    # frame 으로 frame_data 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fdt.* FROM frame_data_temp fdt WHERE fdt.frame_token IN (SELECT ft.token FROM frame_temp ft)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_data_temp")

    # 최초 필터링된 visibility 데이터를 저장하기 위해 visibility_temp 테이블 생성
    query = """
    CREATE TABLE visibility_temp (
    `token` char(1) NOT NULL,
    `level` varchar(7) DEFAULT NULL,
    `description` varchar(49) DEFAULT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    # frame_annotation 으로 visibility 필터링
    query = """
    INSERT INTO visibility_temp
    SELECT v.* FROM visibility v WHERE v.token IN (SELECT fat.visibility_token FROM frame_annotation_temp fat)
    """
    cursor.execute(query)

    # 최초 필터링된 annotation_type 데이터를 저장하기 위해 annotation_type_temp 테이블 생성
    query = """
    CREATE TABLE annotation_type_temp (
    `name` varchar(19) NOT NULL,
    `description` varchar(34) DEFAULT NULL,
    PRIMARY KEY (`name`)
    )
    """
    cursor.execute(query)

    # frame_annotation 으로 annotation_type 필터링
    query = """
    INSERT INTO annotation_type_temp
    SELECT at.* FROM annotation_type at WHERE at.name IN (SELECT fat.annotation_type_name FROM frame_annotation_temp fat)
    """
    cursor.execute(query)

    # frame_annotation 으로 attribute 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT at.* from `attribute_temp` at join
    (SELECT afa.attribute_token from attribute_frame_annotation afa
    join frame_annotation_temp fat on afa.frame_annotation_token = fat.token group by afa.attribute_token) as afa
    on at.token = afa.attribute_token
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "attribute_temp")

    # frame_annotation 으로 instance 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT it.* FROM `instance_temp` it WHERE it.token IN (SELECT fat.instance_token FROM frame_annotation_temp fat)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "instance_temp")

    # instance 로 category 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ct.* FROM category_temp ct WHERE ct.token IN (SELECT it.category_token FROM `instance_temp` it)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "category_temp")

    # dataset 으로 log 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT lt.* FROM log_temp lt WHERE lt.token IN (SELECT dt.log_token FROM dataset_temp dt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "log_temp")

    # 최초 필터링된 map 데이터를 저장하기 위해 map_temp 테이블 생성
    query = """
    CREATE TABLE `map_temp` (
    `token` char(32) NOT NULL,
    `category` char(14) DEFAULT NULL,
    `filename` char(41) DEFAULT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    # log 로 map 필터링
    query = """
    INSERT INTO map_temp
    SELECT m.* FROM `map` m WHERE m.token IN
    (SELECT lm.map_token FROM `dev-adds`.log_map lm WHERE lm.log_token IN (SELECT lt.token FROM log_temp lt))
    """
    cursor.execute(query)

    # dataset 으로 scenario 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT st.* FROM scenario_temp st WHERE st.name IN 
    (SELECT sid.scenario_name FROM scenarios_in_dataset sid WHERE sid.dataset_token IN (SELECT dt.token from dataset_temp dt))
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "scenario_temp")

    # 최초 필터링된 ego_pose 데이터를 저장하기 위해 ego_pose_temp 테이블 생성
    query = """
    CREATE TABLE `ego_pose_temp` (
    `token` char(32) NOT NULL,
    `timestamp` varchar(16) DEFAULT NULL,
    `translation` json DEFAULT NULL,
    `rotation` json DEFAULT NULL,
    `coordinate_system` json DEFAULT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    # frame_data 로 ego_pose 필터링
    query = """
    INSERT INTO ego_pose_temp
    SELECT ep.* FROM ego_pose ep WHERE ep.token IN (SELECT fdt.ego_pose_token FROM frame_data_temp fdt)
    """
    cursor.execute(query)

    # 최초 필터링된 lidarseg 데이터를 저장하기 위해 lidarseg_temp 테이블 생성
    query = """
    CREATE TABLE `lidarseg_temp` (
    `token` char(32) NOT NULL,
    `frame_data_token` char(32) DEFAULT NULL,
    `filename` char(64) DEFAULT NULL,
    PRIMARY KEY (`token`)
    )
    """
    cursor.execute(query)

    # frame_data 로 lidarseg 필터링
    query = """
    INSERT INTO lidarseg_temp
    SELECT l.* from lidarseg l WHERE l.frame_data_token IN (SELECT fdt.token FROM frame_data_temp fdt)
    """
    cursor.execute(query)

    # frame_data 로 calibrated_sensor 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT cst.* FROM calibrated_sensor_temp cst WHERE cst.token IN (SELECT fdt.calibrated_sensor_token FROM frame_data_temp fdt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "calibrated_sensor_temp")

    # calibrated_sensor 로 sensor 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT st.* FROM sensor_temp st WHERE st.token IN (SELECT cst.sensor_token FROM calibrated_sensor_temp cst)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "sensor_temp")

    connection.commit()

    cursor.close()
    connection.close()