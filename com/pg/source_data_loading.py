from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.pg.utils.utility as ut

if __name__ == '__main__':

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']

    for src in src_list:
        src_conf = app_conf[src]

        if src == 'SB':
            print("\nStart reading data from SB:Mysql database")
            txn_df = ut.read_from_mysql(spark, app_secret, src_conf) \
                .withColumn('ins_dt', current_date())

            txn_df.show()

            txn_df.write \
                .mode("append") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("Data loading from SB:Mysql database is completed")

        elif src == 'OL':
            print("\nStart reading data from OL:SFTP Location")
            ol_txn_df = ut.read_from_sftp(spark, app_secret, src_conf,
                                          os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .withColumn('ins_dt', current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("Data loading from OL:SFTP Locationis completed")

        elif src == 'CP':
            print("\nStart reading data from CP:S3 BUcket")
            cp_df = ut.read_from_s3(spark, src_conf) \
                .withColumn('ins_dt', current_date())

            cp_df.show()

            cp_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("Data loading from CP:S3 Bucket is completed")

        elif src == 'ADDR':
            print("\nStart reading data from Mongo DB")
            cust_addr_df = ut.read_from_mongo(spark,src_conf,app_secret) \
                .withColumn('ins_dt', current_date())

            cust_addr_df.printSchema()

            cust_addr_df = cust_addr_df \
                               .select(col("consumer_id"),
                                       col("mobile-no"),
                                       col("address.state").alias("state"),
                                       col("address.city").alias("city"),
                                       col("address.street").alias("street"),
                                       col("ins_dt"))

            cust_addr_df.show()

            cust_addr_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("Data loading from Mongo DB is completed")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/pg/source_data_loading.py