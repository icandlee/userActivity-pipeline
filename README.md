# ETL Data Pipeline: loads user activity log into an external Hive table Using Spark & HDFS

user activity 로그를 Spark, HDFS를 활용하여 Hive 테이블로 제공하는 프로젝트 입니다.

## :bookmark_tabs: Contents

- [About](#about)
- [Features](#Features)
- [Prerequisites](#Prerequisites)
- [Installation](#installation)

## About
- Project Structure
```bash
├─ app
│  └─ src
│     └─ main
│        ├─ java
│        │  └─ com
│        │     └─ etl
│        │        ├─ UserActivityETL.java
│        │        ├─ config
│        │        │  └─ ConfigLoader.java
│        │        ├─ processing
│        │        │  ├─ HiveTableManager.java
│        │        │  └─ UserActivityProcessor.java
│        │        └─ utils
│        │           └─ CheckPointManager.java
│        └─ resources
│           ├─ config.properties 
│           └─ log4j2.xml
├─ gradle
└─ logs

```
- `UserActivityETL.java` : user activity ETL job을 수행하는 main 클래스
- `config` : 데이터 입력/출력 경로 등 properties load 역할을 하는 패키지
- `processing` : Hive external 테이블 생성 & 가공 및 적재 역할을 하는 패키지
- `utils` : 공통 유틸을 포함하는 패키지 / CheckPointManager : chunk 분할 작업시 체크포인트 관리를 위한 클래스

## Features
### 1) KST 기준으로 daily partition 처리
- event_time_kst column 추가 (UTC to KST) 
- event_time_kst의 year, month, day를 기준으로 daily partition 처리

### 2) Hive External Table 방식 설계
- 데이터 적재시 해당 테이블(user_activity)이 없을 경우 자동으로 생성하도록 spark sql로 처리하는 HiveTabeManager.java 클래스 구현

### 3) Data format - parquet, snappy 처리 
- Hive 테이블 설계 및 데이터 저장시에 모두 적용

### 4) 추가 기간 처리에 대응
- Spark에서 DataFrame을 append 모드로 저장하여 기존 데이터 충돌 방지
- 메인 실행시 args 첫 번째 인자로 데이터의 년,월 (ex.2019-10)받아서 처리

### 5) 배치 장애시 복구를 위한 장치 구현
- 대량의 데이터라는 가정하에 데이터를 chunk단위로 나누어 처리하고, 각 청크 처리후 checkPoint 파일에 chunk index 저장
- 작업 실행시 checkPoint 파일을 읽어 마지막으로 처리된 chunk index 다음부터 이어서 처리하도록 구현

## Prerequisites
- Java 8
- Spark 3.5.2 
- Apache Hive 3.1.3
- Hadoop 3.3.6
- PostgreSQL 15.2
- Docker OS : Ubuntu 22.04 
