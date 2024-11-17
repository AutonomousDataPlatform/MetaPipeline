import sys
import pymysql
import logging
import boto3
import pymysql.cursors
import dbinfo
import json
import decimal


def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    bucket_name = 'adds-json'
    prefix = 'batch/'
    output_key = 'frame_annotation.json'

    response = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = prefix)
    
    items = []
    files_processed = 0
    
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.json'):
            file_response = s3_client.get_object(Bucket = bucket_name, Key = obj['Key'])
            file_content = json.loads(file_response['Body'].read().decode('utf-8'))
            items.extend(file_content)
            files_processed += 1
    
    merged_content = json.dumps(items, ensure_ascii=False, indent=2)
    
    s3_client.put_object(
        Bucket = bucket_name,
        Key = 'test/' + output_key,
        Body = merged_content
    )

    return {
            'statusCode':200,
            'body': json.dumps({
                'message': 'Successfully merged JSON files',
                'file_processed': files_processed
            })
    }
