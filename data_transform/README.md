# 데이터 변형
데이터의 용량이 높아 json 파일 형태에서는 `java.lang.OutOfMemoryError: heap space` 오류가 발생하여 csv로 변형하였다.

추가로 파일에서는 문제가 없으나 spark dataframe으로 읽을 때 오류가 발생하는 부분을 수정하였다.
- `review_summary, review_detail`의 경우 `""`로 인해 str을 제대로 인식하지 못하고 `\n`가 존재하는 경우 하나의 str으로 인식하지 못해 삭제.
- `helpful`값은 리스트 형태이므로 str으로 수정.
- `review_date`의 경우 오류와 상관없이 `%Y-%m-%d`로 수정.