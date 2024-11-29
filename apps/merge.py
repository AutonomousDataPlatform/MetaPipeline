import boto3
import json
from botocore.exceptions import BotoCoreError

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    bucket_name = 'adds-json'
    folder = event.get('executionArn')
    prefix = folder + '/batch/'
    output_key = folder + '/frame_annotation.json'

    # 멀티파트 업로드 시작
    try:
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=output_key)
        upload_id = multipart_upload['UploadId']
        part_number = 1
        parts = []

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return {
                'statusCode': 400,
                'body': json.dumps('No files found for merging.')
            }

        buffer = []
        buffer_size = 5 * 1024 * 1024  # 5MB 청크 크기
        files_processed = 0
        is_first_part = True

        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                file_response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                file_content = json.loads(file_response['Body'].read().decode('utf-8'))

                if isinstance(file_content, list):  # 파일 내용이 리스트인지 확인
                    buffer.extend(file_content)
                else:
                    raise ValueError(f"Invalid file format: {obj['Key']} does not contain a list")

                files_processed += 1

                # 청크 크기만큼 버퍼가 찼을 때 업로드
                if len(json.dumps(buffer).encode('utf-8')) >= buffer_size:
                    # JSON 파트 생성: 첫 번째 파트는 '[' 추가, 이후는 ','로 연결
                    if is_first_part:
                        part_body = '[' + json.dumps(buffer, ensure_ascii=False)[1:-1]
                    else:
                        part_body = ',' + json.dumps(buffer, ensure_ascii=False)[1:-1]
                        
                    is_first_part = False  # 이후 파트는 ','로 연결
                    part = s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=output_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=part_body.encode('utf-8')
                    )
                    parts.append({'ETag': part['ETag'], 'PartNumber': part_number})
                    part_number += 1
                    buffer = []  # 버퍼 초기화

        # 남아 있는 데이터 업로드
        if buffer:
            part_body = ('' if is_first_part else ',') + json.dumps(buffer, ensure_ascii=False)[1:-1] + ']'
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=output_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=part_body.encode('utf-8')
            )
            parts.append({'ETag': part['ETag'], 'PartNumber': part_number})


        # 멀티파트 업로드 완료
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=output_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully merged JSON files',
                'files_processed': files_processed
            })
        }

    except BotoCoreError as e:
        # 멀티파트 업로드 중 오류가 발생하면 업로드 중단
        s3_client.abort_multipart_upload(Bucket=bucket_name, Key=output_key, UploadId=upload_id)
        raise e