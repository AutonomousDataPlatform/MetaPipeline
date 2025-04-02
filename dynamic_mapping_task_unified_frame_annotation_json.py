from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pymysql
import json
from datetime import timedelta
import pymysql.cursors
from typing import List, Dict
from airflow.decorators import task
import glob
import os
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner':'eun',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}


@task
def get_chunk_parameters():
    
    def get_total_count():
        conn_info = MySqlHook.get_connection(conn_id='aws_mysql')
        conn = pymysql.connect(
            host=conn_info.host,
            user=conn_info.login,
            password=conn_info.password,
            db='adds',
            port=conn_info.port,
            cursorclass=pymysql.cursors.DictCursor
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM frame_annotation")
        result = cur.fetchone()
        total_count = result['count']
        cur.close()
        conn.close()

        return total_count
    
    total_count=get_total_count()
    num_tasks = (total_count + 199999) // 200000

    return [{"batch_number":i} for i in range(num_tasks)]


def process_batch(batch_number):

    conn_info = MySqlHook.get_connection(conn_id='aws_mysql')

    conn = pymysql.connect(
        host=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        db='adds',
        port=conn_info.port,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    
    page_size = 200000
    offset = batch_number * page_size

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM frame_annotation LIMIT {offset}, {page_size}")
    frame_annotation_results = cur.fetchall()
    
    final_data = []

    for row in frame_annotation_results:
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
            row['attribute_tokens']=json.loads(row['attribute_tokens'])

        final_data.append(row)
    
    cur.close()
    conn.close()
    
    json_file_name = f"unified_json/fa_batch_{batch_number}.json"
    with open(json_file_name, 'w', encoding='utf-8') as json_file:
        json.dump(final_data, json_file, ensure_ascii=False, indent=2)


    return f"Processed batch {batch_number}, saved to unified_json/fa_batch_{batch_number}.json"



def merge_json_files(input_pattern,output_file):
    with open(output_file, 'w', encoding='utf-8') as outfile:
        # 배열의 시작을 씁니다
        outfile.write('[\n')
        
        first_file = True
        for filename in glob.glob(input_pattern):
            print(filename)
            with open(filename, 'r') as infile:
                # 각 파일의 내용을 파싱합니다
                data = json.load(infile)
                
                # 각 항목을 개별적으로 씁니다
                for i, item in enumerate(data):
                    if not first_file or i > 0:
                        outfile.write(',\n')
                    json.dump(item, outfile, ensure_ascii=False, indent=2)
                
                first_file = False

            os.remove(filename)
        
        # 배열의 끝을 씁니다
        outfile.write('\n]')

#고려해야할 부분 merge 함수 작성할 때 위에처럼 fa_*.json 패턴으로 검색해서 합치면 파일 순서가 0~22 가 아니고 뒤죽박죽일 수 있음. 순서 섞여서 합쳐져도 상관없는지 확인
#순서 상관있으면 for i loop 돌면서 fa_batch_{i}.json 여야함.

with DAG(
    'dynamic_mapping_task_unified_frame_annotation_json',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    
    chunk_params = get_chunk_parameters()
    #chunk_params=PythonOperator(task_id='get_chunk_parmas',python_callable=get_chunk_parameters)



    process_batch=PythonOperator.partial(task_id='process_batch', python_callable=process_batch).expand(op_kwargs=chunk_params)

    merge_json_files=PythonOperator(
        task_id='merge_json_files',
        python_callable=merge_json_files,
        op_kwargs={'input_pattern':'unified_json/fa_*.json','output_file':'fa_output.json'}
    )


    chunk_params>>process_batch>>merge_json_files
