from pyspark import SparkContext
import os
import json
import sys

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

def count(reviewRDD):
    count = reviewRDD.count()
    return count

def count_2018(reviewRDD):
    count_2018 = reviewRDD.filter(lambda date: "2018" in date['date']).count()
    return count_2018

def distUser(reviewRDD):
    dist_user = reviewRDD.map(lambda x: x['user_id']).distinct().count()
    return dist_user

def topUser(reviewRDD):
    top_user = reviewRDD.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda x, y: x + y) \
        .takeOrdered(10, lambda x: [-x[1], x[0]])
    return top_user

def distBusiness(reviewRDD):
    dist_business = reviewRDD.map(lambda x: x['business_id']).distinct().count()
    return dist_business

def topBusiness(reviewRDD):
    top_business = reviewRDD.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda x, y: x + y) \
        .takeOrdered(10, lambda x: [-x[1], x[0]])
    return top_business

def saveToOut(output, output_file):
    with open(output_file, 'w') as f:
        json.dump(output, f)

def main():
    input = "data/test_review.json"
    output_file = "output/output1.json"
    sc = SparkContext('local[*]', 'Assignment1')
    sc.setLogLevel("ERROR")

    reviewRDD = sc.textFile(input).map(lambda x: json.loads(x))

    output = {}
    output['n_reviews'] = count(reviewRDD)
    output['n_reviews_2018'] = count_2018(reviewRDD)
    output['n_user'] = distUser(reviewRDD)
    output['top10_user'] = topUser(reviewRDD)
    output['n_business'] = distBusiness(reviewRDD)
    output['top10_business'] = topBusiness(reviewRDD)

    saveToOut(output, output_file)

if __name__ == '__main__':
    main()












