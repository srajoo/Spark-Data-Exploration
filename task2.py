from pyspark import SparkContext
import os
import json
from time import time
import sys

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'


def default(reviewRDD):
    default_start = time()
    top_business = reviewRDD.map(lambda x: (x['business_id'], 1))\
                            .reduceByKey(lambda x, y: x+y)
    default_time = time()-default_start
    default_items = []

    for row in top_business.glom().collect():
        default_items.append(len(row))

    result = {}
    result['n_partition'] = top_business.getNumPartitions()
    result['n_items'] = default_items
    result['exe_time'] = default_time

    return result


def business_partitioner(business_id):
    return hash(business_id)


def customized(reviewRDD, n):
    custom_start = time()
    custom_top_business = reviewRDD.map(lambda x: (x['business_id'], 1))\
                                    .reduceByKey(lambda x, y: x+y)\
                                    .partitionBy(n, business_partitioner)
                                    
    custom_time = time()-custom_start
    custom_items = []
    for row in custom_top_business.glom().collect():
        custom_items.append(len(row))

    result = {}
    result['n_partition'] = custom_top_business.getNumPartitions()
    result['n_items'] = custom_items
    result['exe_time'] = custom_time

    return result

def saveToOut(output, output_file):
    with open(output_file, 'w') as f:
        json.dump(output, f)


def main():
    input_file = "data/test_review.json"
    output_file = "output/output2.json"

    sc = SparkContext('local[*]', 'Assignment1')

    reviewRDD = sc.textFile(input_file).map(lambda x: json.loads(x))
    n = reviewRDD.map(lambda x: x['business_id']).distinct().count()

    output = {}
    output['default'] = default(reviewRDD)
    output['customized'] = customized(reviewRDD, n)

    saveToOut(output, output_file)


if __name__ == '__main__':
    main()



