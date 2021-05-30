from pyspark.sql  import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ = '__main__':
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    #create the spark session
    spark = SpakSession \
        .builder \
        .appName("RDD Examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    curr_dir = os.path.abspath(__file__)
    print(curr_dir)

