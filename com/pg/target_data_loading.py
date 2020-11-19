from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
from pyspark.sql.types import *
import uuid
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

    def fn_uuid():
        uid = uuid.uuid1()
        return str(uid)

    FN_UUID_UDF = spark.udf \
        .register("FN_UUID", fn_uuid, StringType())

    tgt_list = app_conf['target_list']

    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]

        if tgt == 'REGIS_DIM':
            print("REGIS_DIM")
            src_list = tgt_conf['sourceData']
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
                src_df = spark.sql("select * from parquet.`{}`".format(file_path))
                src_df.printSchema()
                src_df.show(5, False)
                src_df.createOrReplaceTempView(src)

            print("REGIS_DIM")

            regis_dim = spark.sql(tgt_conf["loadingQuery"])
            regis_dim.show(5, False)

            ut.write_to_redshift(regis_dim.coalesce(1),
                                 app_secret,
                                 "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                 tgt_conf['tableName'])

        elif tgt == 'CHILD_DIM':
            print("CHILD_DIM")

            src_list = tgt_conf['sourceData']
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
                src_df = spark.sql("select * from parquet.`{}`".format(file_path))
                src_df.printSchema()
                src_df.show(5, False)
                src_df.createOrReplaceTempView(src)

            child_dim = spark.sql(app_conf["CHILD_DIM"]["loadingQuery"])
            child_dim.show(5, False)

            ut.write_to_redshift(child_dim.coalesce(1),
                                 app_secret,
                                 "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                 tgt_conf['tableName'])

        elif tgt == 'RTL_TXN_FACT':

            print("TRX_Fact")

            src_list = tgt_conf['sourceData']
            for src in src_list:
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"][
                    "staging_dir"] + "/" + src
                src_df = spark.sql("select * from parquet.`{}`".format(file_path))
                # src_df.printSchema()
                # src_df.show(5, False)
                src_df.createOrReplaceTempView(src)

            s3_temp_dir = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp"
            src_tbl = tgt_conf['sourceTable']
            for src in src_tbl:
                jdbcUrl = ut.get_redshift_jdbc_url(app_secret)
                print(jdbcUrl)
                txnDf =ut.read_from_redshift(spark, app_secret, s3_temp_dir, src)
                # txnDf.printSchema()
                # txnDf.show(5, False)
                txnDf.createOrReplaceTempView(src.split('.')[1])

            child_dim = spark.sql(tgt_conf['loadingQuery'])
            child_dim.show(5, False)

            ut.write_to_redshift(child_dim.coalesce(1), app_secret, s3_temp_dir, tgt_conf['tableName'])





# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
