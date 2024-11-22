## 1. Insert DB
### Dependencies
- airflow 2.9.3

## Run
1. airflow 설치 후에 생성된 airflow 디렉토리 아래에 dags 폴더 생성
2. unifiedDB 에 있는 dag 파일들을 넣어줌


### Requirements
- airflow 의 connections 는 DAG 실행 전 미리 생성
  - sftp connection (meta data 존재하는 서버)
  - mysql connection (DB 서버)
 
- airflow metadb
  - mysql 이나 postgresql 등으로 변경
 
    *airflow.cfg*
    ```
    sql_alchemy_conn = mysql+mysqldb://<user>:<password>@<host>:3306/<dbname>
    ```
- airflow executor
  - localexecutor 로 변경
 
    *airflow.cfg*
    ```
    executor = LocalExecutor
    ```


## 2. Json Export
### Dependencies
- AWS
- Lambda
- step functions
- serverless framework

### Requirements
- 사용자의 DB 정보를 담는 dbinfo.py 는 각자 생성
    - db_host
    - db_username
    - db_password
    - db_name
    - db_port
