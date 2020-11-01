from pyspark.sql import SparkSession
import yaml
import os.path
import com.pg.utils.utility as ut

from pyspark.sql.functions import current_date

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Data Ingestion") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']

    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            print("\nReading data from mysql")
            txnDf = ut.read_from_mysql(spark, app_secret, src_conf) \
                .withColumn('ins_dt', current_date())

            print("\n Loading to S3 Staging Area>>>>>>>>>>")
            txnDf.write.partitionBy('ins_dt').parquet(app_conf["s3_conf"]["stagining_dir"] + src)
            txnDf.show()

        elif src == 'OL':
            print("\nReading data from sftp")
            ol_txn_df = ut.read_from_sftp(spark, app_secret, src_conf,
                                          os.path.abspath(
                                              current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"])) \
                .ol_txn_df.withColumn("ins_dt", current_date())

            print("\nLoading to S3 Staging Area>>>>>>>>>>")
            ol_txn_df.write.partitionBy("ins_dt").parquet(app_conf["s3_conf"]["staging_dir"] + src)
            ol_txn_df.show()

        elif src == 'ADDR':
            print("\nReading data from mongoDB")
            cust_addr_df = ut.read_from_mongo(spark, app_secret, src_conf) \
                .withColumn("ins_dt", current_date())

            print("\nLoading to S3 Staging Area>>>>>>>>>>")
            cust_addr_df.write.partitionBy("ins_dt").parquet(app_conf["s3_conf"]["staging_dir"] + src)
            cust_addr_df.show(5, False)

        elif src == 'CP':
            print("\nReading data from S3")
            cp_df = ut.read_from_s3(spark, src_conf) \
                .withColumn("ins_dt", current_date())

            print("\nLoading to S3 Staging Area>>>>>>>>>>")
            cp_df.write \
                .partitionBy("ins_dt") \
                .parquet(app_conf["s3_conf"]["staging_dir"] + "/" + "CP")
