import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *


spark = SparkSession \
        .builder \
        .appName("aws_emr_sample_app") \
        .getOrCreate()
        


def main():
    # get details from argv variable from aws lambda
    s3_bucket=sys.argv[1]
    s3_file=sys.argv[2]
    s3_location="s3a://{}/{}".format(s3_bucket,s3_file)
    iris = spark.read.format("csv").option("inferSchema","true").option("header","true").load(s3_location)
    iris.show()
    #ms=iris.groupBy("class").count()
    iris.write.format("csv").option("header", "true").save("s3://<your-processed-file-bucket>/iris.csv")

main()