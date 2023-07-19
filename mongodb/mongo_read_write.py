from pymongo.mongo_client import MongoClient

uri = ""

# Create a new client and connect to the server
client = MongoClient(uri)

# -----------------------------------------------

# read 
# 데이터베이스와 컬렉션 선택
db = client["sample_airbnb"]
collection = db["listingsAndReviews"]

# 쿼리 생성
query = {"_id": "10006546"}

# 컬렉션에서 문서 조회
documents = collection.find(query)

# 조회된 문서 출력
for document in documents:
    print(document)

# -----------------------------------------------    

# write
# 데이터베이스와 컬렉션 선택
db = client["person"]
collection = db["test"]

# 쓸 데이터
data = {
    "name": "skybluelee",
    "salary": 6000
}

# 데이터 삽입
result = collection.insert_one(data)