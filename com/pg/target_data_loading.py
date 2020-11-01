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
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    spark = SparkSession \
        .builder \
        .appName("Ingestion from enterprise applications") \
        .config("spark.mongodb.input.uri", app_secrets_path) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\n Data Loading from CP : S3")
    cp_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + "CP"
    cp_df = spark.sql("select * from parquet.`{}`".format(cp_file_path))
    cp_df.createOrReplaceTempView("CustomerPortal")

    spark.sql("""SELECT DISTINCT REGIS_CNSM_ID, CAST(REGIS_CTY_CODE AS SMALLINT), CAST(REGIS_ID AS INTEGER),
                       REGIS_LTY_ID, REGIS_DATE, REGIS_CHANNEL, REGIS_GENDER, REGIS_CITY, INS_DT
                        FROM
                      CustomerPortal
                    WHERE
                      INS_DT = CURRENT_DATE """) \
        .show(5, False)

    print("\n Data Loading from ADDR : S3")
    cp_file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + "ADDR"
    cp_df = spark.sql("select * from parquet.`{}`".format(cp_file_path))
    cp_df.createOrReplaceTempView("CustomerAddress")

    spark.sql("""
    SELECT 
        DISTINCT consumer_id,'mobile-no' ,state    ,city     ,street,ins_dt
        FROM
        CustomerAddress
        WHERE
         INS_DT = CURRENT_DATE""") \
        .show(5, False)

    print("\n Customer's data with Address")
    spark.sql("""
        SELECT 
                   DISTINCT a.REGIS_CNSM_ID, CAST(a.REGIS_CTY_CODE AS SMALLINT), CAST(a.REGIS_ID AS INTEGER),
                    b.state, b.city,
                   b.street,b.ins_dt
                FROM
                  CustomerPortal a join CustomerAddress b
                  on (a.REGIS_ID=b.REGIS_ID) """) \
        .show(5, False)
