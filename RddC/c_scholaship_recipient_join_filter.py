from pyspark.sql  import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ == '__main__':
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    #create the spark session
    spark = SparkSession \
        .builder \
        .appName("RDD Examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    curr_dir1 = os.path.abspath(os.path.dirname(__file__))





# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" RddC/c_scholaship_recipient_join_filter.py
