from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.pg.utils.utility as ut

if __name__ == "__main__":

    current_dir = os.path(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader= yaml.FullLoader)

    spark = SparkSession \
        .builder \
        .appName("Ingestion from enterprise applications") \
        .config("spark.mongodb.input.uri", app_secrets_path) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']

    for src in src_list:
        src_conf = app_conf[src]

        if src == 'SB':
            print("\n Data Ingestion from SB : MySQL")
            txn_df = ut.read_from_mysql(spark, app_secret, src_conf) \
                .withColumn("ins_dt", current_date())

            txn_df.show()

            txn_df.write \
                .modde('append') \
                .partitionBy('ins_dt') \
                .parquet('s3a://' + app_conf["s3_conf"]["s3_bucket"] + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("\n Data loaded to Staging (s3) >>>>>>>>>>>>")

        elif src == 'OL':
            print("\n Data Ingestion from OL: SFTP")
            ol_txn_df = ut.read_from_sftp(spark, app_secret, src_conf,
                                          os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .withColumn('ins_dt', current_date())

            ol_txn_df.show()

            ol_txn_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("\n Data loaded to Staging (s3) >>>>>>>>>>>>")

        elif src == 'CP':
            print("\n Data Ingestion from CP : S3")
            cp_df = ut.read_from_s3(spark, src_conf) \
                .withColumn('ins_dt', current_date())

            cp_df.write \
                .mode('overwrite') \
                .partitionBy('ins_dt') \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("\n Data loaded to Staging (s3) >>>>>>>>>>>>")


        elif src == "ADDR":
            print("\n Data Ingestion from MongoDB ")
            cust_addr_df = ut.read_from_mongo(spark, src_conf, app_secret) \
                .withColumn('ins_dt', current_date())

            cust_addr_df.write \
                .mode('overwrite') \
                .partitionBy('ins_dt')\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + app_conf["s3_conf"]["staging_dir"] + "/" + src)
            print("\n Data loaded to Staging (s3) >>>>>>>>>>>>")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/pg/source_data_loading.py
