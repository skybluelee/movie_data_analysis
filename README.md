# **실시간 영화 데이터 처리 및 분석**

## **목차**
- [개요](#개요)
- [프로젝트 상세](#프로젝트-상세)
  - [데이터 선정](#데이터-선정)

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
> **사용 기술:** Python, Spark, Kafka

## **프로젝트 상세**
프로젝트 진행 과정에 대한 설명

### 데이터 선정
Kafka producer가 consumer로 전송할 데이터를 영화 리뷰 데이터로 선정. 영화 리뷰 데이터에는 리뷰 수집 날짜가 나와있고 평가 점수와 리뷰를 통해 다양한 분석이 가능하다고 판단하여 이와 같이 결정.

데이터는 Kaggle의 [IMDb Review Dataset](https://www.kaggle.com/datasets/ebiswas/imdb-review-dataset)을 사용.

### 원본 데이터 분석
<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/55e3a877-7702-4f2d-8057-e4eac6312730).png" width="850" height="250"/>

영화 데이터는 6개의 json file로 구성되며 각 파일의 연도는 위와 같다.

실시간 데이터 분석에 모든 데이터를 사용할 필요가 없다고 판단하여 데이터의 양이 많은 2019~2021년에 해당하는 리뷰만을 사용하기로 결정.

<img src="https://github.com/skybluelee/movie_data_analysis/assets/107929903/e7e79129-d5b1-4784-a6b0-858e2aa52321.png" width="900" height="200"/>

모든 데이터에서 년, 월, 일(10일 단위)로 파티셔닝한 상태로 저장.

### 실시간 ETL 구축
