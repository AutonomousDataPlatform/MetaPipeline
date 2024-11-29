import pymysql
import pymysql.cursors
import dbinfo

connection = pymysql.connect(host = dbinfo.db_host, port = 23306, user = dbinfo.db_username, passwd = dbinfo.db_password, db = dbinfo.db_name)

def lambda_handler(event, context):

    cursor = connection.cursor()
    
    query = "drop table annotation_type_temp"
    cursor.execute(query)
    print("annotation_type table drop complete")

    query = "drop table attribute_temp"
    cursor.execute(query)
    print("attribute_temp table drop complete")

    query = "drop table calibrated_sensor_temp"
    cursor.execute(query)
    print("calibrated_sensor_temp table drop complete")

    query = "drop table category_temp"
    cursor.execute(query)
    print("category_temp table drop complete")

    query = "drop table dataset_temp"
    cursor.execute(query)
    print("dataset_temp table drop complete")

    query = "drop table ego_pose_temp"
    cursor.execute(query)
    print("ego_pose_temp table drop complete")

    query = "drop table frame_data_temp"
    cursor.execute(query)
    print("frame_data_temp table drop complete")

    query = "drop table frame_temp"
    cursor.execute(query)
    print("frame_temp table drop complete")

    query = "drop table instance_temp"
    cursor.execute(query)
    print("instance_temp table drop complete")

    query = "drop table lidarseg_temp"
    cursor.execute(query)
    print("lidarseg_temp table drop complete")

    query = "drop table log_temp"
    cursor.execute(query)
    print("log_temp table drop complete")

    query = "drop table map_temp"
    cursor.execute(query)
    print("map_temp table drop complete")

    query = "drop table sensor_temp"
    cursor.execute(query)
    print("sensor_temp table drop complete")

    query = "drop table visibility_temp"
    cursor.execute(query)
    print("visibility_temp table drop complete")

    query = "drop table frame_annotation_temp"
    cursor.execute(query)
    print("frame_annotation_temp table drop complete")

    query = "drop table scenario_temp"
    cursor.execute(query)
    print("scenario_temp table drop complete")
    
    cursor.close()
    connection.close()