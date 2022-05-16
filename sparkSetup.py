import pyspark
import configparser
from delta import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-aws:3.3.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.autoBroadcastJoinThreshold=-1" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.sql.debug.maxToStringFields=100" --master spark://10.1.8.101:7077 pyspark-shell'

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
sparkparam = config_obj["spark"]

# Setup Spark Application
builder = pyspark.sql.SparkSession.builder.appName("pyspark-notebook") \
    .master(sparkparam['master']) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()