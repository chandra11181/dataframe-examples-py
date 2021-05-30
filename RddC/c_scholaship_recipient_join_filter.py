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
    curr_dir1 = os.path.abspath(os.path.dirname(__file__))
    curr_dir2 = os.path.dirname(__file__)
    print("current dirctory using abspath",curr_dir)
    print("current dirctory using abspath and dirname", curr_dir1)
    print("current dirctory using dirname", curr_dir2)

