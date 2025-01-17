from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--package "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark = SparkSession \
        .builder \
        .appName("DSL Examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../"+"application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../../../../"+".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader = yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret = yaml.load(secret, Loader = yaml.FullLoader)

    hadoop_conf =spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    fin_file_path = "s3a://"+ app_conf["s3_conf"]["s3_bucket"] +"/finances-small"
    finance_df=spark.read.parquet(fin_file_path)
    finance_df.printSchema()
    finance_df.show(5, False)

    finance_df \
        .orderBy("Amount") \
        .show(5)

    finance_df \
        .orderBy("Amount") \
        .show(5, False)

    finance_df \
        .withColumn("Account Details", concat_ws(" - ","AccountNumber","Description")) \
        .show(5,False)

    agg_functions_df = finance_df \
        .groupBy("AccountNumber") \
        .agg(avg("Amount").alias("Average Transactions"),
             sum("Amount").alias("Total Transactions"),
             count("Amount").alias("Number of Transactions"),
             max("Amount").alias("Max Transactions"),
             min("Amount").alias("Min of transactions"),
             collect_set("Description").alias("Unique Transaction Description")
    )

    agg_functions_df.show(5, False)

    agg_functions_df \
        .select("accountNumber",
                "Unique Transaction Description",
                size("Unique Transaction Description").alias("CountOfUniqueTransactionDescriptions"),
                sort_array("Unique Transaction Description", False).alias("OrderedUniqueTransactionDescriptions"),
                array_contains("Unique Transaction Description", "Movies").alias("WentToMovie"))\
        .show(5,False)

    companies_df = spark.read.json("s3a://"+app_conf["s3_conf"]["s3_bucket"]+"/company.json")
    print("Count = ", companies_df.count())
    companies_df.show(5,False)
    companies_df.printSchema()
    employee_df_tmp = companies_df \
        .select("company", explode("employees").alias("employee"))
    employee_df_tmp.show()
    companies_df \
        .select("company", posexplode("Employees").alias("employeePosition","employee")) \
        .show()
    employeeDf = employee_df_tmp.select("company", expr("employee.firstName as firstName"))
    employeeDf.show()
    employeeDf.select("*",
                      when(col("company")=="FamilyCo", "Premium")
                      .when(col("company") == "NewCo", "New Company")
                      .when(col("company") == "OldCo", "Old Company")) \
    .show(5,False)



# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" chandra/dataframe1/curation1/dsl1/finance_data_analysis_c.py

