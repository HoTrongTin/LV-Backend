from msilib import schema
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from sparkSetup import spark
from delta.tables import *
from utility import *
import configparser

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
amazonS3param = config_obj["amazonS3"]

#streaming

#d_patient
d_patientsSchema = ( \
    ("subject_id", "integer", False), \
    ("sex", "string"), \
    ("dob", "timestamp"), \
    ("dod", "timestamp"), \
    ("hospital_expire_flg", "string") \
)

def start_d_patient_stream_bronze():
    streamingS3ToBronze(tableName = 'd_patients', schema = d_patientsSchema)

def start_d_patient_stream_silver():
    streamingBronzeToGold(tableName = 'd_patients', schema = d_patientsSchema, mergeOn = ["subject_id"])

#admissions
admissionsSchema = ( \
    ("hadm_id", "integer", False), \
    ("subject_id", "integer", False), \
    ("admit_dt", "timestamp"), \
    ("disch_dt", "timestamp") \
)

def start_admissions_stream_bronze():
    streamingS3ToBronze(tableName = 'admissions', schema = admissionsSchema)

def start_admissions_stream_silver():
    streamingBronzeToGold(tableName = 'admissions', schema = admissionsSchema, mergeOn = ["subject_id", "hadm_id"], partitionedBy = ["subject_id"])

#drgevents
drgeventsSchema = ( \
    ("subject_id", "integer", False), \
    ("hadm_id", "integer", False), \
    ("itemid", "integer", False), \
    ("cost_weight", "float") \
)

def start_drgevents_stream_bronze():
    streamingS3ToBronze(tableName = 'drgevents', schema = drgeventsSchema)

def start_drgevents_stream_silver():
    streamingBronzeToGold(tableName = 'drgevents', schema = drgeventsSchema, mergeOn = ["subject_id", "hadm_id", "itemid"])

#d_codeditems
d_codeditemsSchema = ( \
    ("itemid", "integer", False), \
    ("code", "string"), \
    ("type", "string"), \
    ("category", "string"), \
    ("label", "string"), \
    ("description", "string") \
)

def start_d_codeditems_stream_bronze():
    streamingS3ToBronze(tableName = 'd_codeditems', schema = d_codeditemsSchema)

def start_d_codeditems_stream_silver():
    streamingBronzeToGold(tableName = 'd_codeditems', schema = d_codeditemsSchema, mergeOn = ["itemid"], partitionedBy = ["type"])

#demographic_detail
demographic_detailSchema = ( \
    ("subject_id", "integer", False), \
    ("hadm_id", "integer", False), \
    ("marital_status_itemid", "integer"), \
    ("marital_status_descr", "string"), \
    ("ethnicity_itemid", "integer"), \
    ("ethnicity_descr", "string"), \
    ("overall_payor_group_itemid", "integer"), \
    ("overall_payor_group_descr", "string"), \
    ("religion_itemid", "integer"), \
    ("religion_descr", "string"), \
    ("admission_type_itemid", "integer"), \
    ("admission_type_descr", "string"), \
    ("admission_source_itemid", "integer"), \
    ("admission_source_descr", "string") \
)

def start_demographic_detail_stream_bronze():
    streamingS3ToBronze(tableName = 'demographic_detail', schema = demographic_detailSchema)

def start_demographic_detail_stream_silver():
    streamingBronzeToGold(tableName = 'demographic_detail', schema = demographic_detailSchema, mergeOn = ["subject_id", "hadm_id"])

#icd9
icd9Schema = ( \
    ("subject_id", "integer", False), \
    ("hadm_id", "integer", False), \
    ("sequence", "integer", False), \
    ("code", "string"), \
    ("description", "string") \
)

def start_icd9_stream_bronze():
    streamingS3ToBronze(tableName = 'icd9', schema = icd9Schema)

def start_icd9_stream_silver():
    streamingBronzeToGold(tableName = 'icd9', schema = icd9Schema, mergeOn = ["subject_id", "hadm_id", "sequence"], partitionedBy = ["subject_id"])

def init_spark_streaming():
    print('init streaming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", amazonS3param['s3aImpl'])
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
    start_demographic_detail_stream_bronze()
    start_demographic_detail_stream_silver()
    start_icd9_stream_bronze()
    start_icd9_stream_silver()