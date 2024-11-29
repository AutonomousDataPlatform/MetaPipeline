import boto3
import json

def lambda_handler(event, context):
    sfn_client = boto3.client('stepfunctions')
    
    # 요청 본문(body) 가져오기
    body = event.get('body', '')

    # 본문이 JSON 형식으로 전달되었으므로 JSON으로 파싱
    data = json.loads(body)
    
    execution_arn = data.get('execution_arn')

    try:
        # 실행 상태 확인
        response = sfn_client.describe_execution(
            executionArn=execution_arn
        )
        
        # 상태 정보 반환
        status = response['status']
        return {
            'statusCode': 200,
            'body': json.dumps({
                'executionArn': execution_arn,
                'status': status
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }