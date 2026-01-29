import json
import boto3
import logging
from datetime import datetime

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 客户端
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    """处理 S3 文件上传事件"""
    try:
        logger.info("事件: %s", json.dumps(event))
        
        # 解析 S3 事件
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info("处理文件: s3://%s/%s", bucket, key)
        
        # 下载文件
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        
        # 处理数据
        processed_data = process_data(file_content)
        
        # 保存结果
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"processed/{timestamp}/{key.split('/')[-1]}.json"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(processed_data),
            ContentType='application/json'
        )
        
        # 启动 Glue Job
        job_response = glue_client.start_job_run(
            JobName='DataProcessingJob',
            Arguments={
                '--input_path': f"s3://{bucket}/{output_key}",
                '--output_path': f"s3://{bucket}/output/",
                '--job-bookmark-option': 'job-bookmark-enable'
            }
        )
        
        logger.info("Glue Job 已启动: %s", job_response['JobRunId'])
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '处理成功',
                'job_run_id': job_response['JobRunId'],
                'output_location': f"s3://{bucket}/{output_key}"
            })
        }
        
    except Exception as e:
        logger.error("错误: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def process_data(raw_data):
    """处理原始数据"""
    try:
        # 如果是 JSON 数据
        if raw_data.strip().startswith('{') or raw_data.strip().startswith('['):
            data = json.loads(raw_data)
        else:
            # CSV 或其他文本数据
            data = {'raw_content': raw_data, 'lines': raw_data.count('\n')}
        
        # 添加元数据
        processed = {
            'data': data,
            'processed_at': datetime.now().isoformat(),
            'record_count': len(data) if isinstance(data, list) else 1
        }
        
        return processed
        
    except Exception as e:
        return {'error': str(e), 'raw_data_preview': raw_data[:100]}