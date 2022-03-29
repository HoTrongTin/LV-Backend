from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from sparkSetup import spark
from delta.tables import *

#streaming
def start_d_patient_stream():
    d_patientsSchema = StructType() \
        .add("subject_id", "string") \
        .add("sex", "string") \
        .add("dob", "timestamp") \
        .add("dod", "timestamp") \
        .add("hospital_expire_flg", "string")

    dfD_patients = spark.readStream.option("sep", ",").option("header", "true").schema(d_patientsSchema).csv("s3a://sister-team/spark-streaming/medical/d_patients").withColumn('Date_Time', current_timestamp())

    def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
        # df.write.format('delta').outputMode("append").option("checkpointLocation", "/medical/bronze/d_patients/checkpointD_patients").start("/medical/bronze/d_patients")
        df.write.format("delta").mode("append").save("/medical/bronze/d_patients")

        deltaTable = DeltaTable.forPath(spark, "/medical/silver/d_patients")
        deltaTable.alias("sink").merge(
            df.alias("src"),
            "sink.subject_id = src.subject_id") \
        .whenMatchedUpdate(set = { 
            "sex" : "src.sex",
            "dob" : "src.dob",
            "dod" : "src.dod",
            "hospital_expire_flg" : "src.hospital_expire_flg",
            "Date_Time" : "src.Date_Time",
            } ) \
        .whenNotMatchedInsertAll().execute()
  
    dfD_patients.writeStream.option("checkpointLocation", "/medical/bronze/d_patients/checkpointD_patients").outputMode("append").foreachBatch(foreach_batch_function).start()


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

    start_d_patient_stream()
    #start_admission_stream()