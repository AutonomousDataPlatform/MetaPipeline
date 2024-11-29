import boto3
import zipfile
import io

def lambda_handler(event, context):
    bucket_name = 'adds-json'
    executionArn = event.get('executionArn')

    prefix = executionArn + '/'

    s3_client = boto3.client('s3')

    # List objects in the source bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix = prefix)
    objects = response.get('Contents', [])

    # ZIP 파일을 메모리에서 생성
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for obj in objects:
            key = obj.get('Key')
            # Get object content
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read()

            zip_file.writestr(key, content)
    
    zip_buffer.seek(0)
    zip_file_key = executionArn + '.zip'

    s3_client.put_object(Bucket = bucket_name, Key = zip_file_key, Body = zip_buffer)