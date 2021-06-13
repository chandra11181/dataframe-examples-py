from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from model.Role import Role
from model.Employee import Employee

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .config("spark.sql.crossJoin.enabled", "true")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    emp_df = spark.createDataFrame([
        Employee(1, "Sidhartha", "Ray"),
        Employee(2, "Pratik", "Solanki"),
        Employee(3, "Ashok", "Pradhan"),
        Employee(4, "Rohit", "Bhangur"),
        Employee(5, "Kranti", "Meshram"),
        Employee(7, "Ravi", "Kiran")
    ])

    role_df = spark.createDataFrame([
        Role(1, "Architect"),
        Role(2, "Programmer"),
        Role(3, "Analyst"),
        Role(4, "Programmer"),
        Role(5, "Architect"),
        Role(6, "CEO")
    ])

    # employeeDf.join(empRoleDf, "id" === "id").show(false)   #Ambiguous column name "id"
    emp_df.join(role_df, emp_df.id == role_df.id).show(5, False)

    print("without on condition..")
    emp_df.join(role_df).show(100, False)

    emp_df.join(broadcast(role_df), emp_df["id"] == role_df["id"]).show(5, False)


    # Join Types: "left_outer"/"left", "full_outer"/"full"/"outer"
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "inner").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "right_outer").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "left_anti").show()
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "full").show()

    # cross join
    emp_df.join(role_df, [emp_df["id"] == role_df["id"]], "cross").show()

    print("cross without on condition..")
    emp_df.join(role_df, "cross").show(100, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/curation/dsl/joins_demo.py
