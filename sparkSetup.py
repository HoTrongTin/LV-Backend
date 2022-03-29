import pyspark
import configparser

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