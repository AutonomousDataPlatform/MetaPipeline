import boto3
import json
import os
import boto3
import json
import logging
import os

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # 요청 본문(body) 가져오기
    body = event.get('body', '')

    content = json.loads(body)
    expiration_time = content['ExpiresIn']

    sfn_client = boto3.client('stepfunctions')

    stage = os.environ.get('STAGE','dev')

    if stage == 'dev':
        state_machine_arn = "arn:aws:states:ap-northeast-2:339713009546:stateMachine:BatchProcessStateMachineStepFunctionsStateMachine-4sXvt5is6aNM"

    elif stage == 'prod':
        state_machine_arn = "arn:aws:states:ap-northeast-2:339713009546:stateMachine:BatchProcessStateMachineStepFunctionsStateMachine-s4HnrqyQOZXn"
        
    sfn_reponse = sfn_client.start_execution(
        stateMachineArn = state_machine_arn,
        input = body
    )

    bucket_name = 'adds-json'
    zip_file_key = sfn_reponse['executionArn'] + '.zip'

    s3 = boto3.client('s3', region_name = 'ap-northeast-2')

    presigned_url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket_name, "Key": zip_file_key},
                    ExpiresIn=expiration_time  #  URL 유효기간
                )

    reponse = {'statusCode' : 200, 
               'body' : json.dumps({'presignedURL' : presigned_url, 'executionArn' : sfn_reponse['executionArn']})}

    return reponse