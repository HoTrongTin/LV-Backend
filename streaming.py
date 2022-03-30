from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from sparkSetup import spark
from delta.tables import *

#streaming
def start_d_patient_stream_bronze():
    d_patientsSchema = StructType() \
        .add("subject_id", "string") \
        .add("sex", "string") \
        .add("dob", "timestamp") \
        .add("dod", "timestamp") \
        .add("hospital_expire_flg", "string")

    dfD_patients = spark.readStream.option("sep", ",").option("header", "true").schema(d_patientsSchema).csv("s3a://sister-team/spark-streaming/medical/d_patients").withColumn('Date_Time', current_timestamp())
    dfD_patients.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/d_patients").start("/medical/bronze/d_patients")

def start_d_patient_stream_silver():

    d_patientsSchema = StructType() \
        .add("sujbect_id", "string") \
        .add("sex", "string") \
        .add("dob", "timestamp") \
        .add("dod", "timestamp") \
        .add("hospital_expire_flg", "string")
    
    def upsertToDelta(microBatchOutputDF, batchId): 
        # Set the dataframe to view name
        microBatchOutputDF.createOrReplaceTempView("updates")
        if not(DeltaTable.isDeltaTable(spark, '/medical/silver/d_patients')):
            spark.sql("CREATE TABLE silver_d_patients (subject_id string, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/d_patients'")
        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/d_patients` silver_d_patients
            USING updates s
            ON silver_d_patients.subject_id = s.subject_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    dfD_patients = spark.readStream.format("delta").load("/medical/bronze/d_patients")
  
    dfD_patients.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/d_patients").outputMode("update").foreachBatch(upsertToDelta).start()


def start_admission_stream():
    admissionsSchema = StructType() \
    .add("hadm_id", "string") \
    .add("subject_id", "string") \
    .add("admit_dt", "string") \
    .add("disch_dt", "string")

    dfAdmissions = spark.readStream.option("sep", ",").option("header", "true").schema(admissionsSchema).csv("s3a://sister-team/spark-streaming/medical/admissions").withColumn('Date_Time', current_timestamp())
    dfAdmissions \
    .writeStream \
    .format('delta') \
    .outputMode("append") \
    .option("checkpointLocation", "/bronze/admissions/checkpointAdmissions") \
    .start("/bronze/admissions")

def init_spark_streaming():
    print('init streaming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "AKIASIV2BBOBY7OLXVET")
    hadoop_conf.set("fs.s3a.secret.key", "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")

    start_d_patient_stream_bronze()
    start_d_patient_stream_silver()
    #start_admission_stream()