from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == '__main__'
    os.environ["PY_SPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadopp-aws:2.7.4" pyspark-shell'
    )
    spark = SparkSession \
        .builder \
        .appName("DSL Examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir+"/../../../"+ ".secrets")

    conf = open(app_config_path)
    app_conf =yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader = yaml.fullLoader)

    hadoop_conf = spark.SparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app.secret["s3_conf"]["secret_access_key"])

    fin_file_path ="s3a://"+app_conf["s3_conf"]["s3_bucket"] + "/finances-small"
    finance_df = spark.read.parquet(fin_file_path)

    finance_df.printSchema()
    finance_df.show(5,False)






# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" chandra/dataframe/curation/dsl/finance_data_analysis_c.py