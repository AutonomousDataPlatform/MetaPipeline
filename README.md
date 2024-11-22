## 1. Insert DB
### Dependencies
- airflow 2.9.3

### Requirements
- airflow 의 connections 는 DAG 실행 전 미리 생성


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
