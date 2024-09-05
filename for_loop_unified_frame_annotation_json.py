from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymysql
import json
from datetime import timedelta
import pymysql.cursors


default_args = {
    'owner':'eun',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

def test0():
    test=0
    print(test)


def test1():
    test=1
    print(test)

def test2():
    test=2
    print(test)

@task
def test3():
    test=3
    
    return test

@task
def test4(test):
    print(test)
    
    return test

def calculate(**kwargs):

    #print(f'kwargs: {kwargs}')
    ti=kwargs['ti']
    print(f'ti:{ti}')
    calculate=ti.xcom_pull(key='return_value', task_ids='test3')
    calculate=ti.xcom_pull(task_ids='test3')
    print(f'calculate: {calculate}')
    result=calculate+10

    print(result)

@task
def task_decoration(i):
    print(i['i'])



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


tasks=[]
with DAG(
    'for_loop_unified_frame_annotation_json',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    

    total_count=get_total_count()
    #print(f'total_row_count: {total_count}')

    num_tasks = (total_count + 199999) // 200000
    #get_total_count=PythonOperator(task_id='get_total_count', python_callable=get_total_count)


    for i in range(num_tasks):
        task=PythonOperator(
            task_id=f'process_batch_{i}',
            python_callable=process_batch,
            op_kwargs={'batch_number':i}
        )
        tasks.append(task)



    #task=task_decoration({'i':1, 'j':2})

    #t3>>t4


#궁금한거 : airflow 파이썬 코드 짤 때 task 생성 용도아니고 반환값 쓰려고 호출용도로 함수 만들기도 하는지(위에서는 get_total_count 함수), 일반 파이썬처럼

"""
    t0=PythonOperator(task_id='test0',python_callable=test0)
    t2=PythonOperator(task_id='test2',python_callable=test2)
    t3=PythonOperator(task_id='test3',python_callable=test3)

    def task_in_function():
        t1=PythonOperator(task_id='test1',python_callable=test1)

        return t1

    t1=task_in_function()


t0>>t1>>t2>>t3
"""

#함수 안에서 task 생성 가능 ? task 생성해서 의존 관계 설정 가능 ? (함수 안에서 task 생성하는 거 통상적으로 자주 쓰인다고 했는데 진짜인지 자료를 찾아봐야함)
#task 생성하고 실행하는 방법에 대해서도 명확히 알 필요 있음. task 를 만들어놓기만 해도 실행이 되는 건지, operator 로 task 를 만들면 어떤일이 벌어지고, 이 task 를 실행하려면 어떻게 해야하는지 
#의존 관계도 조각조각 설정해주면 이어붙여주는지
#예를 들어 t1>>t2 , t2>>t3 , t0>>t1 하면 UI에서는 t0>>t1>>t2>>t3 로 ?
#조각조각 설정해주어도 블럭처럼 이어붙여주는데, t1>>t2, t3>>t2 이런식으로 의존관계 설정하면 t3가 t1,t2 사이에 들어가는 것이 아니라 각 각 설정됨
#t1>>t2, t3>>t1 인 경우에는 t3>>t1>>t2 이렇게 엮임

#함수 내부에서 operator 로 task 생성하는 것은 적절하지 않은 방식
    
    #process_results = process_batch.expand(batch_number=range(num_tasks))
    