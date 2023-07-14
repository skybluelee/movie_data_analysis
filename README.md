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
![year](https://github.com/skybluelee/movie_data_analysis/assets/107929903/55e3a877-7702-4f2d-8057-e4eac6312730)

