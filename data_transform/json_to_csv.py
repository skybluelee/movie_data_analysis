import json
import csv
from dateutil.parser import parse

def sentence_filter(sentence):
    for idx, value in enumerate(sentence):
        if value == '"':
            sentence = sentence[:idx] + "'" + sentence[idx + 1:]
        elif value =="\n":
            sentence = sentence[:idx] + " " + sentence[idx + 1:]
    return sentence

with open('D:\sentiment-analyisis-in-real_time\movie_dataset\part-06.json', 'r') as f:
    json_data = json.load(f)

for data in json_data:
    try:
        good, bad = data['helpful'][0], data['helpful'][1]
        data['helpful'] = good + ',' + bad
    except:
        pass
    try:
        date_origin = data['review_date']
        data['review_date'] = parse(date_origin).strftime('%Y-%m-%d')
    except:
        pass
    try:
        review_origin = data['review_detail']
        data['review_detail'] = sentence_filter(review_origin)
    except:
        pass
    try:
        review_sum_origin = data['review_summary']
        data['review_summary'] = sentence_filter(review_sum_origin)
    except:
        pass

input_file_name = json_data
output_file_name = "part-06.csv"
with open(output_file_name, "w", encoding="utf-8", newline="") as output_file:
    data = []
    for line in json_data:
        datum = line
        data.append(datum)
        
    csvwriter = csv.writer(output_file)
    csvwriter.writerow(data[0].keys())
    for line in data:
        csvwriter.writerow(line.values())