# **실시간 영화 데이터 처리 및 분석**

## **목차**
- [개요](#개요)
- [프로젝트 상세](#프로젝트-상세)
  - [데이터 선정](#데이터-선정)
  - [원본 데이터 분석](#원본-데이터-분석)
  - [실시간 ETL 구축](#실시간-ETL-구축)
  - [Streaming](#Streaming)
  - [Consumer to MongoDB](#Consumer-to-MongoDB)
  - [MongoDB to Tableau](#MongoDB-to-Tableau)

## **개요**

> **프로젝트:** 실시간 영화 데이터 처리 및 분석
>
> **기획 및 제작:** 이상민
>
> **분류:** 개인 프로젝트
>
> **작업 기간:** 2023.07.07 ~ 2023.07.20
>
> **주요 기능:** 실시간 데이터 파이프라인 구축, 데이터 분석
>
> **사용 기술:** Python, Spark, Kafka, AWS, MongDB Atlas, Tableau, Zeppelin

## **프로젝트 상세**
스트리밍에 사용할 데이터 분석 및 선정 후 실시간 상황을 가정하여 데이터를 Kafka를 사용하여 실시간 파이프라인 구축, consumer에서 데이터를 필터링하여 MongoDB에 전송하고 Tableau와 연동하여 데이터 시각화 진행

### 데이터 선정
Kafka producer가 consumer로 전송할 데이터를 영화 리뷰 데이터로 선정. 영화 리뷰 데이터에는 리뷰 수집 날짜가 나와있고 평가 점수와 리뷰를 통해 다양한 분석이 가능하다고 판단하여 이와 같이 결정.

데이터는 Kaggle의 [IMDb Review Dataset](https://www.kaggle.com/datasets/ebiswas/imdb-review-dataset)을 사용.

### 원본 데이터 분석
<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/55e3a877-7702-4f2d-8057-e4eac6312730.png" width="1000" height="400"/>

영화 데이터는 6개의 json file로 구성되며 각 파일의 연도는 위와 같다.

실시간 데이터 분석에 모든 데이터를 사용할 필요가 없다고 판단하여 데이터의 양이 많은 2019~2021년에 해당하는 리뷰만을 사용하기로 결정.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/e7e79129-d5b1-4784-a6b0-858e2aa52321.png" width="900" height="260"/>

모든 데이터에서 년, 월, 일(10일 단위)로 파티셔닝한 상태로 저장.

### 실시간 ETL 구축
zookeeper와 kafka를 설치하고 `kafka/config` 내의 `zookeeper.properties, server.properties`에서 자신이 사용할 인스턴스를 전부 등록.

이후 `nohup_zookeeper.out &, nohup_kafka.out &` 명령을 사용하여 zookeeper, kafka를 실행.

`jps` 명령을 통해 `QuorumPeerMain`, `kafka`가 동작 중인지 확인 필요. 이유 없이 exit 상태가 되는 경우가 자주 있는데, 사용하는 인스턴스에서 전부 동작중인 상태에서 스트리밍 구현 가능.

`--create --topic`을 통해 topic을 생성하고 `kafka-console-producer.sh`, `kafka-console-consumer.sh` 명령을 통해 실시간 스트리밍을 구현.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/fa74b9bc-c6d1-4480-ae75-6373b6fc07e3.png" width="1000" height="600"/>

producer가 topic을 향해 데이터를 전송하는 경우, 그리고 consumer가 topic의 데이터를 받는 경우 Python을 사용해 kafka 모듈의 KafkaProducer, KafkaConsumer를 사용하는 경우와 pyspark를 사용해 writeStream, readStream을 사용하는 경우 2가지가 존재.

Spark를 사용하는 경우 writeStream과 readStream을 사용하기 위해서는 기존의 dataframe을 streaming dataframe으로 변환해야 함.

Producer의 경우 python을 사용하여 각 데이터를 전송하였으며, Consumer의 경우 이미 topic으로 들어오는 데이터 자체가 streaming dataframe이므로 readStream을 통해 데이터를 읽음.

2가지 방식은 kafka 디렉토리에 작성.
### Streaming
<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/2e48574c-6134-4d89-b085-abae854144aa.png" width="800" height="1000"/>

streaming 방식은 Structured Streaming이며 위의 요소를 참고하여 batch size, acks, retries, partition 등의 parameter를 조절하며 최적화를 진행해야 함.
### Consumer to MongoDB
<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/9df0ddb7-1d54-4bf7-adac-52ffd5fe2407.png" width="800" height="250"/>

이후 사용할 Tableau와 연동을 위해 보다 간편한 MongoDB Atlas를 사용.
[Structured Streaming with MongoDB](https://www.mongodb.com/docs/spark-connector/current/structured-streaming/)에 Spark를 사용하여 MongoDB로 writeStream하는 방식이 나와있다.

Kafka에서 readStream으로 읽은 dataframe을 MongoDB에 json 형태로 정확하게 전송하기 위해서는 해당 dataframe을 json 형태로 parsing 하는 과정이 필요. 만약 parsing하지 않는다면 `value: ""a": 1, "b": "id""`와 같이 json 형태의 하나의 string으로 전송될 가능성이 존재.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/f1c845fc-bca5-4e06-84ac-4900fa8f4dc1.png" width="1000" height="700"/>

Atlas에 들어가서 Browse Collections를 누르면 해당 database의 collection에서 데이터를 조회할 수 있고 `{field: 'value'}` 형태의 쿼리를 날릴 수 있음.

Connect에서 driver를 클릭하면 uri를 확인할 수 있고 Atlas SQL에서 기타 BI Tool와 연동이 가능, Shell 에서는 최신 MongoDB를 설치한 CMD에서 기존 MongoDB와 같이 쿼리를 날리는 것이 가능.
### MongoDB to Tableau
[How to Connect Tableau with MongoDB](https://www.mongodb.com/features/mongodb-tableau)를 참고하여 따라하면 `MongoDB Atlas X MongoDB`가 추가되는데, uri, id, password만으로 간편하게 연동할 수 있음.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/6f2f45f5-cc19-4d65-a657-9397c1cbfd2d.png" width="1000" height="600"/>

테이블을 연결하면 아래와 같이 필드와 테이블이 존재. 간혹 Tableau가 제대로 데이터를 불러오지 못하는 경우가 존재하는데, 본인 Database에 문제가 없다면 시간의 여유를 가지고 기다려보자.

데이터가 load되었음을 확인하면 시트로 들어가서 시각화가 가능. 

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/4573dd10-433f-426e-a5ca-688e26fc8407.png" width="1000" height="300"/>

좌측의 테이블에서 사용하고자 하는 데이터를 행과 열로 옮겨 데이터를 시각화할 수 있음. 처음에는 단순 표만 나오는데 우측 상단의 '표현 방식'을 이용해 다양한 지표로 바꾸어 사용 가능.
DESC, ASC 정렬이 가능하고 행 또는 열의 데이터를 더블 클릭하면 해당 데이터에 대해 집계 함수 사용 가능.

데이터를 연동하는 경우 Tableau는 모든 데이터를 Database에서 가지고 옴. 나의 경우 데이터가 별로 없는 초기에는 데이터를 가지고 오는 시간이 적어 데이터의 변화를 실시간으로 확인이 가능했으나, 데이터의 양이 많아지면서 실시간으로 확인하는 것이 불가능했고 프로그램도 무거워져 시트를 조작하는데 상당한 시간이 소요. 필요한 데이터만을 가지고 오는 것이 필요.
