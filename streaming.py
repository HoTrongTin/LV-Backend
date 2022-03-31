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

#d_patient
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
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/d_patients` silver_d_patients
            USING updates s
            ON silver_d_patients.subject_id = s.subject_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/d_patients')):
        spark.sql("CREATE TABLE silver_d_patients (subject_id integer, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/d_patients'")
    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/d_patients')):
        spark.sql("CREATE TABLE bronze_d_patients (subject_id integer, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/d_patients'")

    dfD_patients = spark.readStream.format("delta").load("/medical/bronze/d_patients")
  
    dfD_patients.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/d_patients").outputMode("update").foreachBatch(upsertToDelta).start()

#admissions
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
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/admissions` silver_admissions
            USING updates s
            ON silver_admissions.subject_id = s.subject_id AND silver_admissions.hadm_id = s.hadm_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/admissions')):
        spark.sql("CREATE TABLE silver_admissions (hadm_id integer, subject_id integer, admit_dt timestamp, disch_dt timestamp, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/admissions' PARTITIONED BY (subject_id)")
    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/admissions')):
        spark.sql("CREATE TABLE bronze_admissions (hadm_id integer, subject_id integer, admit_dt timestamp, disch_dt timestamp, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/admissions'")

    dfadmissions = spark.readStream.format("delta").load("/medical/bronze/admissions")
  
    dfadmissions.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/admissions").outputMode("update").foreachBatch(upsertToDelta).start()

#drgevents
def start_drgevents_stream_bronze():
    drgeventsSchema = StructType() \
        .add("subject_id", "integer") \
        .add("hadm_id", "integer") \
        .add("itemid", "integer") \
        .add("cost_weight", "float")

    dfdrgevents = spark.readStream.option("sep", ",").option("header", "true").schema(drgeventsSchema).csv(amazonS3param['s3aURL'] + "/medical/drgevents").withColumn('Date_Time', current_timestamp())
    dfdrgevents.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/drgevents").start("/medical/bronze/drgevents")

def start_drgevents_stream_silver():
    
    def upsertToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/drgevents` silver_drgevents
            USING updates s
            ON silver_drgevents.subject_id = s.subject_id AND silver_drgevents.hadm_id = s.hadm_id AND silver_drgevents.itemid = s.itemid
            WHEN MATCHED THEN UPDATE SET silver_drgevents.cost_weight = s.cost_weight
            WHEN NOT MATCHED THEN INSERT *
        """)

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/drgevents')):
        spark.sql("CREATE TABLE silver_drgevents (subject_id integer, hadm_id integer, itemid integer, cost_weight float, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/drgevents'")
    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/drgevents')):
        spark.sql("CREATE TABLE bronze_drgevents (subject_id integer, hadm_id integer, itemid integer, cost_weight float, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/drgevents'")

    dfdrgevents = spark.readStream.format("delta").load("/medical/bronze/drgevents")
  
    dfdrgevents.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/drgevents").outputMode("update").foreachBatch(upsertToDelta).start()

#d_codeditems
def start_d_codeditems_stream_bronze():
    d_codeditemsSchema = StructType() \
        .add("itemid", "integer") \
        .add("code", "string") \
        .add("type", "string") \
        .add("category", "string") \
        .add("label", "string") \
        .add("description", "string")

    dfd_codeditems = spark.readStream.option("sep", ",").option("header", "true").schema(d_codeditemsSchema).csv(amazonS3param['s3aURL'] + "/medical/d_codeditems").withColumn('Date_Time', current_timestamp())
    dfd_codeditems.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/d_codeditems").start("/medical/bronze/d_codeditems")

def start_d_codeditems_stream_silver():
    
    def upsertToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/d_codeditems` silver_d_codeditems
            USING updates s
            ON silver_d_codeditems.itemid = s.itemid
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/d_codeditems')):
        spark.sql("CREATE TABLE silver_d_codeditems (itemid integer, code string, type string, category string, label string, description string, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/d_codeditems' PARTITIONED BY (type)")
    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/d_codeditems')):
        spark.sql("CREATE TABLE bronze_d_codeditems (itemid integer, code string, type string, category string, label string, description string, Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/d_codeditems'")

    dfd_codeditems = spark.readStream.format("delta").load("/medical/bronze/d_codeditems")
  
    dfd_codeditems.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/d_codeditems").outputMode("update").foreachBatch(upsertToDelta).start()


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
    start_drgevents_stream_bronze()
    start_drgevents_stream_silver()
    start_d_codeditems_stream_bronze()
    start_d_codeditems_stream_silver()