import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import os


default_args={
        'owner':'eun',
        'retries':1,
        'retry_delay':timedelta(minutes=5),
}


kakao_remote_path='/data/3D_object_detection_tracking(autonomous_driving)'
nuScenes_remote_path='/data/nuScenes/v1.0-mini'


#sftp connection 을 이용하여 파일 다운로드
#kakao frame_annotation 테이블 데이터 삽입
def kakao_frame_annotation_insert():

    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    kakao_directory_list=hook.list_directory(kakao_remote_path)
    #local 에 저장할 때 파일 이름
    kakao_local_path='kakao_frame_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")


    # attribute 테이블에서 name과 token 가져오기
    query = "SELECT name, token FROM attribute"
    cur.execute(query)

    # 결과를 딕셔너리에 저장
    attribute_data=cur.fetchall()
    attribute_dict = {name: token for name, token in attribute_data}

    with hook.get_conn() as sftp_client:

        #kakao dataset 테이블 데이터 삽입
        #kakao는 여러개의 데이터셋이 다른 디렉토리에 개별로 저장되어있어서 디렉토리 돌아야하고, nuScenes 는 하나의 디렉토리에 여러개의 데이터셋이 한번에 들어있음
        for dir_name in kakao_directory_list:

            kakao_file_path=kakao_remote_path + '/' + dir_name + '/meta/frame_annotation.json'

            print(f"{kakao_file_path} get start")
            sftp_client.get(kakao_file_path, kakao_local_path)
            print(f"{kakao_file_path} get end")

            with open(kakao_local_path) as f:

                print(f"{kakao_file_path} load start")
                json_data=json.load(f)
                print(f"{kakao_file_path} load end")

                data=[]
                

                print(len(json_data))

                for i in range(len(json_data)):

                    

                    frame_annotation_data = json_data[i]

                    uuid=frame_annotation_data['uuid']
                    token=uuid.replace("-","")

                    frame_data_uuid=frame_annotation_data['frame_data_uuid']
                    frame_data_token=frame_data_uuid.replace("-","")

                    """
                    #kakao 는 frame_data_token 로 맵핑하기때문에 nuScenes 처럼 frame_token 데이터도 넣어주기 위해 frame_data_token 을 이용해서 frame_token 가져옴
                    sql="SELECT frame_token FROM frame_data WHERE token=%s"
                    param=(frame_data_token,)

                    #print("frame_token get start")
                    cur.execute(sql,param)
                    result=cur.fetchone()
                    #print("frame_token get end")

                    frame_token=result[0]
                    """

                    frame_data_token=frame_data_uuid.replace("-","")

                    instance_uuid=frame_annotation_data['instance_uuid']
                    instance_token=instance_uuid.replace("-","")

                    # is_visible 로 visibility_token(kakao에서의 visibility_level) 값을 넣어주기 위해서
                    # visibility 테이블의 token 값을 is_visible 에 맞게 할당함
                    # bbox_pcd3d 인 경우 geometry 에 is_visible 정보가 없기 때문에, visibility_token 정보를 취득할 수 없음. 따라서 null 처리
                    is_visible=frame_annotation_data['geometry'].get('is_visible')

                    if is_visible==None:
                        visibility_token=None

                    elif is_visible is True:
                        visibility_token="5"

                    elif is_visible is False:
                        visibility_token="0"
                    
                    annotation_type_name=frame_annotation_data['annotation_type_name']
                    is_lidar_synced=frame_annotation_data['is_lidar_synced']

                    # wlh, center, orientation, bbox_image3d 는 데이터에 따라서 geometry 데이터가 다르기때문에 값이 있을수도 없을 수도 있음
                    # 따라서 값이 없을때는 json 문자열로 바꿔주는 것이 아니라 None 값으로 놔둬야함
                    size_wlh=frame_annotation_data['geometry'].get('wlh')
                    if size_wlh:
                        size_wlh=json.dumps(size_wlh)

                    translation_center=frame_annotation_data['geometry'].get('center')
                    if translation_center:
                        translation_center=json.dumps(translation_center)

                    rotation_orientation=frame_annotation_data['geometry'].get('orientation')
                    if rotation_orientation:
                        rotation_orientation=json.dumps(rotation_orientation)

                    bbox_image3d=frame_annotation_data['geometry'].get('bbox_image3d')
                    if bbox_image3d:
                        bbox_image3d=json.dumps(bbox_image3d)

                    num_lidar_pts=frame_annotation_data['num_pts']


                    #attribute 의 데이터를 가공해서 name 을 만들어 준 후 name 에 해당하는 token 을 가져와서 배열에 넣어줌. attribute_tokens 이 attribute 에 해당하는 토큰 배열이므로 그 형태 만들어줌

                    if frame_annotation_data['attribute'] is not None:

                        # 데이터 한 행마다 attribute_tokens 배열은 초기화 되어야함
                        # 한 instance(= annotation 하나) 가 가지고있는 attribute 에 대한 데이터이기 때문
                        result_attribute_token=[]

                        for key, value in frame_annotation_data['attribute'].items():
                            # '_state'가 키에 포함되어 있는지 확인
                            if '_state' in key:
                                # '_state'를 제거
                                new_key = key.replace('_state', '')
                            else:
                                # '_state'가 없으면 그대로 사용
                                new_key = key
                            
                            # 키와 값을 연결하여 새로운 문자열 생성
                            new_name = f"{new_key}.{value}"

                            attribute_token=attribute_dict.get(new_name)
                            result_attribute_token.append(attribute_token)

                        result_attribute_token=json.dumps(result_attribute_token)

                    else:
                        result_attribute_token = None

                    
                    frame_aligned=frame_annotation_data['frame_aligned']
                    description=frame_annotation_data['description']

                    if frame_annotation_data['prev']:
                        prev=frame_annotation_data['prev'].replace("-","")
                    else:
                        prev=None

                    if frame_annotation_data['next']:
                        next=frame_annotation_data['next'].replace("-","")
                    else:
                        next=None


                    # 처음 데이터 넣어줄때는 frame_data_token 으로 frame_token select 하면 오래걸리기 때문에 frame_token 은 넣어주지않음
                    #모든 데이터 넣어준 후에 temp 테이블을 이용해서 한번에 insert 해줌
                    data.append((token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, result_attribute_token, frame_aligned, description, prev, next))
                    #print("data append")

                    #50000개 단위로 insert
                    if (i % 50000 == 0 and i>0):

                        print("50000 data append end")

                        query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                        print("%d executemany start" %(i//50000))
                        cur.executemany(query,data)
                        print("%d executemany end" %(i//50000))

                        data = []

                # 나머지 데이터 삽입 ( 마지막 50000개까지 안 채워진 데이터 )
                query = "INSERT IGNORE INTO frame_annotation (token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                print("last executemany start")
                cur.executemany(query,data)
                print("last executemany end")

                #commit은 트랜잭션 고려해서 한 데이터셋에 대한 모든 데이터가 insert 됐을 때 마지막에 실행
                print("commit start")
                conn.commit()
                print("insert end")
        

        #최종 frame_annotation 테이블와 동일한 temp 테이블을 만듦
        query="CREATE TEMPORARY TABLE IF NOT EXISTS temp SELECT fa.token, fd.frame_token, fa.frame_data_token,fa.instance_token, fa.visibility_token,fa.annotation_type_name, fa.is_lidar_synced, fa.size_wlh, fa.translation_center, fa.rotation_orientation, fa.bbox_image3d, fa.num_lidar_pts, fa.attribute_tokens, fa.frame_aligned, fa.description, fa.prev, fa.`next` FROM frame_annotation fa JOIN frame_data fd ON fa.frame_data_token=fd.token"

        cur.execute(query)

        query="DELETE FROM frame_annotation"
        cur.execute(query)
        conn.commit()

        #temp 테이블을 그대로 frame_annotation 에 넣어줌
        query="INSERT INTO frame_annotation(token, frame_token, frame_data_token, instance_token, visibility_token, annotation_type_name, is_lidar_synced, size_wlh, translation_center, rotation_orientation, bbox_image3d, num_lidar_pts, attribute_tokens, frame_aligned, description, prev, next) SELECT * FROM temp"

        cur.execute(query)
        conn.commit()

    cur.close()
    conn.close()
    os.remove(kakao_local_path)


#nuScenes frame_annotation 테이블 데이터 삽입
def nuScenes_frame_annotation_insert():
    #sftp 접속 정보
    hook=SFTPHook(ssh_conn_id="eun_sftp")
    #local 에 저장할 때 파일 이름
    nuScenes_local_path='nuScenes_frame_annotation.json'

    #mysql 접속 정보
    mysql_hook=MySqlHook.get_hook(conn_id='eun_mysql')
    conn=mysql_hook.get_conn()
    cur=conn.cursor()

    cur.execute("use metaDB")

    with hook.get_conn() as sftp_client:

        nuScenes_file_path=nuScenes_remote_path + '/sample_annotation.json'
        sftp_client.get(nuScenes_file_path, nuScenes_local_path)

        with open(nuScenes_local_path) as f:
            json_data=json.load(f)

            data=[]

            for sample_annotation_data in json_data:

                token=sample_annotation_data['token']
                frame_token=sample_annotation_data['sample_token']
                instance_token=sample_annotation_data['instance_token']
                visibility_token=sample_annotation_data['visibility_token']
                size_wlh=sample_annotation_data['size']
                translation_center=sample_annotation_data['translation']
                rotation_orientation=sample_annotation_data['rotation']
                num_lidar_pts=sample_annotation_data['num_lidar_pts']
                num_radar_pts=sample_annotation_data['num_radar_pts']

                # attribute_tokens 가 [] 빈배열 값일때, null 처리
                if sample_annotation_data['attribute_tokens']:
                    attribute_tokens=json.dumps(sample_annotation_data['attribute_tokens'])
                else:
                    attribute_tokens=None

                if sample_annotation_data['prev']=="":
                    prev=None
                else:
                    prev=sample_annotation_data['prev'].replace("-","")
                    

                if sample_annotation_data['next']=="":
                    next=None
                else:
                    next=sample_annotation_data['next'].replace("-","")

                
                data.append((token, frame_token, instance_token, visibility_token, json.dumps(size_wlh), json.dumps(translation_center), json.dumps(rotation_orientation), num_lidar_pts, num_radar_pts, attribute_tokens, prev, next))

            query="INSERT IGNORE INTO frame_annotation (token, frame_token, instance_token, visibility_token, size_wlh, translation_center, rotation_orientation, num_lidar_pts, num_radar_pts, attribute_tokens, prev, next) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.executemany(query,data)
            conn.commit()
        

    cur.close()
    conn.close()
    os.remove(nuScenes_local_path)


with DAG('unified_frame_annotation_insert',
        default_args=default_args,
        schedule_interval=None,
        )as dag:
    kakao_frame_annotation_insert=PythonOperator(task_id='kakao_frame_annotation_insert', python_callable=kakao_frame_annotation_insert)
    nuScenes_frame_annotation_insert=PythonOperator(task_id='nuScenes_frame_annotation_insert', python_callable=nuScenes_frame_annotation_insert)


kakao_frame_annotation_insert >> nuScenes_frame_annotation_insert