# **실시간 영화 데이터 처리 및 분석**

## **목차**
- [개요](#개요)
- [프로젝트 상세](#프로젝트-상세)
  - [데이터 선정](#데이터-선정)
  - [원본 데이터 분석](#-원본 데이터 분석)
  - [실시간 ETL 구축](#-실시간 ETL 구축)

## **개요**

> **프로젝트:** 실시간 영화 데이터 처리 및 분석
>
> **기획 및 제작:** 이상민
>
> **분류:** 개인 프로젝트
>
> **작업 기간:** 2023.07 ~
>
> **주요 기능:** 실시간 데이터 파이프라인 구축, 데이터 분석
>
> **사용 기술:** Python, Spark, Kafka, AWS, MongDB Atlas, Tableau, Zeppelin

## **프로젝트 상세**
프로젝트 진행 과정에 대한 설명

### 데이터 선정
Kafka producer가 consumer로 전송할 데이터를 영화 리뷰 데이터로 선정. 영화 리뷰 데이터에는 리뷰 수집 날짜가 나와있고 평가 점수와 리뷰를 통해 다양한 분석이 가능하다고 판단하여 이와 같이 결정.

데이터는 Kaggle의 [IMDb Review Dataset](https://www.kaggle.com/datasets/ebiswas/imdb-review-dataset)을 사용.

### 원본 데이터 분석
<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/55e3a877-7702-4f2d-8057-e4eac6312730.png" width="850" height="450"/>

영화 데이터는 6개의 json file로 구성되며 각 파일의 연도는 위와 같다.

실시간 데이터 분석에 모든 데이터를 사용할 필요가 없다고 판단하여 데이터의 양이 많은 2019~2021년에 해당하는 리뷰만을 사용하기로 결정.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/e7e79129-d5b1-4784-a6b0-858e2aa52321.png" width="900" height="260"/>

모든 데이터에서 년, 월, 일(10일 단위)로 파티셔닝한 상태로 저장.

### 실시간 ETL 구축
zookeeper와 kafka를 설치하고 `kafka/config` 내의 `zookeeper.properties, server.properties`에서 자신이 사용할 인스턴스를 전부 등록.

이후 `nohup_zookeeper.out &, nohup_kafka.out &` 명령을 사용하여 zookeeper, kafka를 실행.

`jps` 명령을 통해 `QuorumPeerMain`, `kafka`가 동작 중인지 확인 필요. 이유 없이 exit 상태가 되는 경우가 자주 있는데, 사용하는 인스턴스에서 전부 동작중인 상태에서 스트리밍 구현 가능.

`--create --topic`을 통해 topic을 생성하고 `kafka-console-producer.sh`, `kafka-console-consumer.sh` 명령을 통해 실시간 스트리밍을 구현.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/fa74b9bc-c6d1-4480-ae75-6373b6fc07e3.png" width="900" height="260"/>

producer가 topic을 향해 데이터를 전송하는 경우, 그리고 consumer가 topic의 데이터를 받는 경우 Python을 사용해 kafka 모듈의 KafkaProducer, KafkaConsumer를 사용하는 경우와 pyspark를 사용해 writeStream, readStream을 사용하는 경우 2가지가 존재.

Spark를 사용하는 경우 writeStream과 readStream을 사용하기 위해서는 기존의 dataframe을 streaming dataframe으로 변환해야 함.

Producer의 경우 python을 사용하여 각 데이터를 전송하였으며, Consumer의 경우 이미 topic으로 들어오는 데이터 자체가 streaming dataframe이므로 readStream을 통해 데이터를 읽음.

2가지 방식은 kafka 디렉토리에 작성.
### Consume
