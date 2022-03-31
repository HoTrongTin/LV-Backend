from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from sparkSetup import spark
from delta.tables import *
import configparser

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
amazonS3param = config_obj["amazonS3"]

#streaming
def start_d_patient_stream_bronze():
    d_patientsSchema = StructType() \
        .add("subject_id", "integer") \
        .add("sex", "string") \
        .add("dob", "timestamp") \
        .add("dod", "timestamp") \
        .add("hospital_expire_flg", "string")

    dfD_patients = spark.readStream.option("sep", ",").option("header", "true").schema(d_patientsSchema).csv(amazonS3param['s3aURL'] + "/medical/d_patients").withColumn('Date_Time', current_timestamp())
    dfD_patients.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/d_patients").start("/medical/bronze/d_patients")

def start_d_patient_stream_silver():

    def upsertToDelta(microBatchOutputDF, batchId): 
        # Set the dataframe to view name
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/d_patients` silver_d_patients
            USING updates s
            ON silver_d_patients.subject_id = s.subject_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/d_patients')):
        spark.sql("CREATE TABLE silver_d_patients (subject_id integer, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/d_patients'")
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/d_patients')):
        spark.sql("CREATE TABLE bronze_d_patients (subject_id integer, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/d_patients'")

    dfD_patients = spark.readStream.format("delta").load("/medical/bronze/d_patients")
  
    dfD_patients.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/d_patients").outputMode("update").foreachBatch(upsertToDelta).start()

def start_admissions_stream_bronze():
    admissionsSchema = StructType() \
        .add("hadm_id", "integer") \
        .add("subject_id", "integer") \
        .add("admit_dt", "timestamp") \
        .add("disch_dt", "timestamp")

    dfadmissions = spark.readStream.option("sep", ",").option("header", "true").schema(admissionsSchema).csv(amazonS3param['s3aURL'] + "/medical/admissions").withColumn('Date_Time', current_timestamp())
    dfadmissions.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/admissions").start("/medical/bronze/admissions")

def start_admissions_stream_silver():
    
    def upsertToDelta(microBatchOutputDF, batchId): 
        # Set the dataframe to view name
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/admissions` silver_admissions
            USING updates s
            ON silver_admissions.subject_id = s.subject_id AND silver_admissions.hadm_id = s.hadm_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/admissions')):
        spark.sql("CREATE TABLE silver_admissions (hadm_id integer, subject_id integer, admit_dt timestamp, disch_dt timestamp, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/admissions'")
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/admissions')):
        spark.sql("CREATE TABLE bronze_admissions (hadm_id integer, subject_id integer, admit_dt timestamp, disch_dt timestamp, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/admissions' PARTITIONED BY (subject_id)")

    dfadmissions = spark.readStream.format("delta").load("/medical/bronze/admissions")
  
    dfadmissions.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/admissions").outputMode("update").foreachBatch(upsertToDelta).start()

def init_spark_streaming():
    print('init streaming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", amazonS3param['accesskey'])
    hadoop_conf.set("fs.s3a.secret.key", amazonS3param['secretkey'])

    start_d_patient_stream_bronze()
    start_d_patient_stream_silver()
    start_admissions_stream_bronze()
    start_admissions_stream_silver()