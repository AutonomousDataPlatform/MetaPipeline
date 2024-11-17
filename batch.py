import sys
import pymysql
import logging
import boto3
import pymysql.cursors
import dbinfo
import json
import decimal

connection=pymysql.connect(host=dbinfo.db_host, port=23306, user=dbinfo.db_username, passwd=dbinfo.db_password, db=dbinfo.db_name)

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Decimal을 float으로 변환
    raise TypeError


def lambda_handler(event, context):

    bucket='adds-json'
    json_file_name = f"frame_annotation_{event['batch_number']}.json"

    page_size = 200000
    offset = event['batch_number'] * page_size

    cursor=connection.cursor(pymysql.cursors.DictCursor)
    
    query = "SELECT * FROM frame_annotation_temp LIMIT %s, %s"
    cursor.execute(query, (offset, page_size))
    
    result = cursor.fetchall()
    
    final_data = []

    for row in result:
        if row['annotation_type_name'] == 'bbox_pcd3d':
            row['geometry'] = {
                'wlh': json.loads(row['size_wlh']),
                'center': json.loads(row['translation_center']),
                'orientation': json.loads(row['rotation_orientation'])
            }
            del row['size_wlh'], row['translation_center'], row['rotation_orientation'], row['bbox_image3d']

        elif row['annotation_type_name'] == 'bbox_image3d':
            row['geometry'] = json.loads(row['bbox_image3d'])
            del row['size_wlh'], row['translation_center'], row['rotation_orientation'], row['bbox_image3d']

        elif row['annotation_type_name'] == 'nuScenes_annotation':
            row['geometry'] = {
                'size': json.loads(row['size_wlh']),
                'translation': json.loads(row['translation_center']),
                'rotation': json.loads(row['rotation_orientation'])
            }
            del row['size_wlh'], row['translation_center'], row['rotation_orientation'], row['bbox_image3d']

        if row['attribute_tokens']:
            row['attribute_tokens'] = json.loads(row['attribute_tokens'])

        final_data.append(row)


    content = json.dumps(final_data, ensure_ascii = False, indent = 2, default = default).encode('UTF-8')
    
    s3 = boto3.client('s3', region_name = 'ap-northeast-2')
    s3.put_object(Bucket = bucket, Key = 'batch/' + json_file_name, Body = content)

    cursor.close()
    connection.close()
    
    response = {"statusCode":200, "body": json.dumps("success")}

    return response