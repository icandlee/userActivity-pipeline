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
│        │           └─ FileUtils.java
│        └─ resources
│           ├─ config.properties 
│           └─ log4j2.xml
├─ gradle
└─ logs
   ├─ spark-job-2024-09-17.log
   └─ spark-job.log
```
- `UserActivityETL.java` : user activity ETL job을 수행하는 main 클래스
- `config` : 데이터 입력/출력 경로 등 properties load 역할을 하는 패키지
- `processing` : Hive external 테이블 생성 & 가공 및 적재 역할을 하는 패키지

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
- Spark Streaming 방식 및 체크포인트로 장애 발생시 해당 지점부터 다시 처리 가능하도록 구현 

## Prerequisites
- Java 8
- Spark 3.5.2 
- Apache Hive 3.1.3
- Hadoop 3.3.6
- PostgreSQL 15.2
- Docker OS : Ubuntu 22.04 

## Installation

Install hive
```bash

```
