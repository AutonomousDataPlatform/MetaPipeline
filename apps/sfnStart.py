import boto3
import json
import os
import logging
import requests
from datetime import datetime, timezone, timedelta

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_slack_webhook_url():
    ssm = boto3.client('ssm', region_name='ap-northeast-2')
    parameter_name = os.environ.get("SLACK_WEBHOOK_PARAMETER_NAME", "/slack/bigdata_aws_monitoring/webhook_url")
    
    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )
    return response['Parameter']['Value']

def convert_to_kst(request_time):
    """
    미국 시간 기준의 request_time (예: '21/May/2025:03:25:32 +0000')을 한국 시간(KST)으로 변환
    """
    utc_time = datetime.strptime(request_time, "%d/%b/%Y:%H:%M:%S +0000")
    kst = timezone(timedelta(hours=9))
    return utc_time.replace(tzinfo=timezone.utc).astimezone(kst).strftime("%Y-%m-%d %H:%M:%S")

def lambda_handler(event, context):
    # (1) Slack Webhook URL 가져오기
    slack_webhook_url = get_slack_webhook_url()

    # (2) 요청 정보 파싱
    request_time = event.get('requestContext', {}).get('requestTime', '')
    api_path = event.get('requestContext', {}).get('path', 'N/A')
    request_body = event.get('body', '{}')
    
    # (3) 한국 시간 포맷 변환
    formatted_time = convert_to_kst(request_time) if request_time else "N/A"

    # (4) 슬랙 메시지 구성 (cost 알림 스타일)
    slack_message = {
        "text": (
            "🚀 *API 호출 알림*\n"
            f"- 경로 : `{api_path}`\n"
            f"- 요청 시간 : `{formatted_time}`\n"
            f"- 요청 데이터 : ```\n{request_body}\n```"
        )
    }

    # (5) 슬랙 메시지 전송
    try:
        response = requests.post(
            slack_webhook_url,
            data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            logger.error(f"Slack 전송 실패. 응답 코드: {response.status_code}, 내용: {response.text}")
        else:
            logger.info("Slack 전송 성공")
    except Exception as e:
        logger.error(f"Slack 메시지 전송 오류: {str(e)}")

    # (6) Step Functions 실행
    body = request_body
    content = json.loads(body)
    expiration_time = content.get('ExpiresIn', 3600)  # 기본 1시간

    sfn_client = boto3.client('stepfunctions')
    stage = os.environ.get('STAGE', 'dev')

    if stage == 'dev':
        state_machine_arn = "arn:aws:states:ap-northeast-2:339713009546:stateMachine:BatchProcessStateMachineStepFunctionsStateMachine-4sXvt5is6aNM"
    else:
        state_machine_arn = "arn:aws:states:ap-northeast-2:339713009546:stateMachine:BatchProcessStateMachineStepFunctionsStateMachine-s4HnrqyQOZXn"

    sfn_response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=body
    )

    # (7) Presigned URL 생성
    s3 = boto3.client('s3', region_name='ap-northeast-2')
    bucket_name = 'adds-json'
    zip_file_key = sfn_response['executionArn'] + '.zip'

    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": zip_file_key},
        ExpiresIn=expiration_time
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'presignedURL': presigned_url,
            'executionArn': sfn_response['executionArn']
        })
    }
