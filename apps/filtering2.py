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
    query = "DROP TABLE temp_filtered"
    cursor.execute(query)

def lambda_handler(event, context):

    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.execute("select count(*) from temp_filtered")
    result = cursor.fetchone()
    print(result)
    
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
    print("frame_data 로 frame 필터링")

    # dataset 으로 frame 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ft.* FROM frame_temp ft WHERE ft.dataset_token IN (SELECT dt.token FROM dataset_temp dt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_temp")
    print("dataset 으로 frame 필터링")

    # frame_annotaton 으로 frame 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ft.* FROM frame_temp ft WHERE ft.token IN (SELECT fat.frame_token FROM frame_annotation_temp fat)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_temp")
    print("frame_annotaton 으로 frame 필터링")

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
    print("frame 으로 frame_annotation 필터링")

    # frame 으로 dataset 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT * FROM dataset_temp dt WHERE dt.token IN (SELECT ft.dataset_token FROM frame_temp ft)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "dataset_temp")
    print("frame 으로 dataset 필터링")
    
    # frame 으로 frame_data 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT fdt.* FROM frame_data_temp fdt WHERE fdt.frame_token IN (SELECT ft.token FROM frame_temp ft)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "frame_data_temp")
    print("frame 으로 frame_data 필터링")

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
    print("frame_annotation 으로 visibility 필터링")

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
    print("frame_annotation 으로 annotation_type 필터링")

    # frame_annotation 으로 attribute 필터링
    query = """
    CREATE TABLE `temp_filtered` (
    `token` char(32) DEFAULT NULL,
    `name` varchar(35) DEFAULT NULL,
    `description` varchar(100) DEFAULT NULL
    )
    """
    cursor.execute(query)

    query = """
    CREATE TEMPORARY TABLE afa_temp AS
    SELECT afa.attribute_token 
    FROM attribute_frame_annotation afa
    JOIN frame_annotation_temp fat 
    ON afa.frame_annotation_token = fat.token
    GROUP BY afa.attribute_token
    """
    cursor.execute(query)

    query = """
    INSERT INTO temp_filtered
    SELECT at.* FROM attribute_temp at
    JOIN afa_temp aft
    ON at.token = aft.attribute_token
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
    print("frame_annotation 으로 instance 필터링")

    # instance 로 category 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT ct.* FROM category_temp ct WHERE ct.token IN (SELECT it.category_token FROM `instance_temp` it)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "category_temp")
    print("instance 로 category 필터링")

    # dataset 으로 log 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT lt.* FROM log_temp lt WHERE lt.token IN (SELECT dt.log_token FROM dataset_temp dt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "log_temp")
    print("dataset 으로 log 필터링")

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
    print("log 로 map 필터링")

    # dataset 으로 scenario 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT st.* FROM scenario_temp st WHERE st.name IN 
    (SELECT sid.scenario_name FROM scenarios_in_dataset sid WHERE sid.dataset_token IN (SELECT dt.token from dataset_temp dt))
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "scenario_temp")
    print("dataset 으로 scenario 필터링")

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
    print("frame_data 로 ego_pose 필터링")

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
    print("frame_data 로 lidarseg 필터링")

    # frame_data 로 calibrated_sensor 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT cst.* FROM calibrated_sensor_temp cst WHERE cst.token IN (SELECT fdt.calibrated_sensor_token FROM frame_data_temp fdt)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "calibrated_sensor_temp")
    print("frame_data 로 calibrated_sensor 필터링")

    # calibrated_sensor 로 sensor 필터링
    query = """
    CREATE TEMPORARY TABLE temp_filtered AS
    SELECT st.* FROM sensor_temp st WHERE st.token IN (SELECT cst.sensor_token FROM calibrated_sensor_temp cst)
    """
    cursor.execute(query)

    replace_table_with_temp(cursor, "sensor_temp")
    print("calibrated_sensor 로 sensor 필터링")

    connection.commit()
    
    cursor.close()
    connection.close()