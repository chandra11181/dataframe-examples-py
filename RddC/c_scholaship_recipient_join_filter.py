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
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf_path = os.path.abspath(curr_dir + "/../"+"application.yml")
    app_secrets_path = os.path.abspath(curr_dir + "/../"+".secrets")

    conf = open(app_conf_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_sec = yaml.load(secret, Loader=yaml.FullLoader)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_sec["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.access.key", app_sec["s3_conf"]["secret_access_key"])
    demographics_rdd = spark.sparkContext.testFile("s3a://"+app_conf["s3_conf"]["s3_bucket"]+ "/demographic.csv")
    finances_rdd = spark.sparkContext.testFile("s3a://" + app_conf["s3_conf"]["s3_bucket"]+ "/finances.csv")
    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4]))))
    finance_rdd_pair = finances_rdd \
        .map(lambda rec: rec.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4]))))
    print('Participants belongs to \'Switzerland\', having debts and financial dependents,')
    join_pair_rdd = demographics_pair_rdd \
        .join(finance_rdd_pair) \
        .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][1][0] == 1) and (rec[1][1][1] == 1))

    join_pair_rdd.foreach(print)





# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" RddC/c_scholaship_recipient_join_filter.py
