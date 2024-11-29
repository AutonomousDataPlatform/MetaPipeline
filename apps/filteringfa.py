import boto3
import pymysql
import pymysql.cursors
import dbinfo

def lambda_handler(event, context):
    connection=pymysql.connect(host=dbinfo.db_host, port=23306, user=dbinfo.db_username, passwd=dbinfo.db_password, db=dbinfo.db_name)

    page_size = 1000000
    offset = event['batch_number'] * page_size

    cursor = connection.cursor(pymysql.cursors.DictCursor)

    query = """
    CREATE TEMPORARY TABLE afa_temp
    SELECT afa.frame_annotation_token FROM attribute_frame_annotation afa WHERE afa.attribute_token IN (SELECT at.token FROM `attribute_temp` at) LIMIT %s, %s
    """
    cursor.execute(query, (offset, page_size))

    query = """
    INSERT INTO temp_filtered
    SELECT fat.* FROM frame_annotation_temp fat WHERE fat.token IN
    (SELECT aft.frame_annotation_token FROM afa_temp aft)
    """
    cursor.execute(query)

    connection.commit()

    cursor.close()
    connection.close()