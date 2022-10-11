# Spark-Data-Exploration

In this repo, you can find source code for the exploration of Yelp Dataset using Spark RDD. The original data set can be found here. A subset of the review dataset has been
uploaded in the data folder. This code has been scaled to process larger datasets (eg: datasets ovr 5GB).

1. task1.py performes data exploration and answers the following questions:

A. The total number of reviews
B. The number of reviews in 2018
C. The number of distinct users who wrote reviews
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
E. The number of distinct businesses that have been reviewed
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had

The output of the above code can be found in the output folder.

2.  Processing large volumes of data requires performance decisions and properly partitioning the data for processing is imperative. In task2.py I have
shown the number of partitions and the number of items per partition used to find "The top 10 businesses that had the largest numbers of reviews and the number of reviews they had".
I also implemented a custom parition function that uses hashing to improve the performance of the map and reduce tasks. A comparison can also be seen in 
the output. 
