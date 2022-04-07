from sparkSetup import spark
from utility import *
import configparser

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
amazonS3param = config_obj["amazonS3"]

#streaming

#merge method
#d_patients 3952
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
    streamingBronzeToGoldMergeMethod(tableName = 'd_patients', schema = d_patientsSchema, mergeOn = ["subject_id"])

#admissions 5074
admissionsSchema = ( \
    ("hadm_id", "integer", False), \
    ("subject_id", "integer", False), \
    ("admit_dt", "timestamp"), \
    ("disch_dt", "timestamp") \
)

def start_admissions_stream_bronze():
    streamingHDFSToBronze(tableName = 'admissions', schema = admissionsSchema)

def start_admissions_stream_silver():
    streamingBronzeToGoldMergeMethod(tableName = 'admissions', schema = admissionsSchema, mergeOn = ["hadm_id"], partitionedBy = ["subject_id"])

#drgevents 5055
drgeventsSchema = ( \
    ("subject_id", "integer", False), \
    ("hadm_id", "integer", False), \
    ("itemid", "integer", False), \
    ("cost_weight", "float") \
)

def start_drgevents_stream_bronze():
    streamingS3ToBronze(tableName = 'drgevents', schema = drgeventsSchema)

def start_drgevents_stream_silver():
    streamingBronzeToGoldMergeMethod(tableName = 'drgevents', schema = drgeventsSchema, mergeOn = ["hadm_id"])

#d_codeditems 3339
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
    streamingBronzeToGoldMergeMethod(tableName = 'd_codeditems', schema = d_codeditemsSchema, mergeOn = ["itemid"], partitionedBy = ["type"])

#demographic_detail 5074
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
    streamingBronzeToGoldMergeMethod(tableName = 'demographic_detail', schema = demographic_detailSchema, mergeOn = ["hadm_id"])

#icd9 53486
icd9Schema = ( \
    ("subject_id", "integer", False), \
    ("hadm_id", "integer", False), \
    ("sequence", "integer", False), \
    ("code", "string"), \
    ("description", "string") \
)

def start_icd9_stream_bronze():
    streamingHDFSToBronze(tableName = 'icd9', schema = icd9Schema)

def start_icd9_stream_silver():
    streamingBronzeToGoldMergeMethod(tableName = 'icd9', schema = icd9Schema, mergeOn = ["hadm_id", "sequence"], partitionedBy = ["subject_id"])

#icustay_days 34730
icustay_daysSchema = ( \
    ("icustay_id", "integer", False), \
    ("subject_id", "integer", False), \
    ("seq", "integer", False), \
    ("begintime", "timestamp"), \
    ("endtime", "timestamp"), \
    ("first_day_flg", "string"), \
    ("last_day_flg", "string") \
)

def start_icustay_days_stream_bronze():
    streamingHDFSToBronze(tableName = 'icustay_days', schema = icustay_daysSchema)

def start_icustay_days_stream_silver():
    streamingBronzeToGoldMergeMethod(tableName = 'icustay_days', schema = icustay_daysSchema, mergeOn = ["icustay_id", "seq"], partitionedBy = ["subject_id"])

#icustay_detail 5810
icustay_detailSchema = ( \
    ("icustay_id", "integer", False), \
	("subject_id", "integer", False), \
	("gender", "string"), \
	("dob", "timestamp"), \
	("dod", "timestamp"), \
	("expire_flg", "string"), \
	("subject_icustay_total_num", "integer"), \
	("subject_icustay_seq", "integer"), \
	("hadm_id", "integer"), \
	("hospital_total_num", "integer"), \
	("hospital_seq", "integer"), \
	("hospital_first_flg", "string"), \
	("hospital_last_flg", "string"), \
	("hospital_admit_dt", "timestamp"), \
	("hospital_disch_dt", "timestamp"), \
	("hospital_los", "integer"), \
	("hospital_expire_flg", "string"), \
	("icustay_total_num", "integer"), \
	("icustay_seq", "integer"), \
	("icustay_first_flg", "string"), \
	("icustay_last_flg", "string"), \
	("icustay_intime", "timestamp"), \
	("icustay_outtime", "timestamp"), \
	("icustay_admit_age", "float"), \
	("icustay_age_group", "string"), \
	("icustay_los", "integer"), \
	("icustay_expire_flg", "string"), \
	("icustay_first_careunit", "string"), \
	("icustay_last_careunit", "string"), \
	("icustay_first_service", "string"), \
	("icustay_last_service", "string"), \
	("height", "float"), \
	("weight_first", "float"), \
	("weight_min", "float"), \
	("weight_max", "float"), \
	("sapsi_first", "integer"), \
	("sapsi_min", "integer"), \
	("sapsi_max", "integer"), \
	("sofa_first", "integer"), \
	("sofa_min", "integer"), \
	("sofa_max", "integer"), \
	("matched_waveforms_num", "integer") \
)

def start_icustay_detail_stream_bronze():
    streamingHDFSToBronze(tableName = 'icustay_detail', schema = icustay_detailSchema)

def start_icustay_detail_stream_silver():
    streamingBronzeToGoldMergeMethod(tableName = 'icustay_detail', schema = icustay_detailSchema, mergeOn = ["icustay_id"], partitionedBy = ["subject_id"])

#append method
#chartevents
charteventsSchema = ( \
    ("subject_id", "integer", False), \
    ("icustay_id", "integer"), \
    ("itemid", "integer", False), \
    ("charttime", "timestamp"), \
    ("elemid", "integer"), \
    ("realtime", "timestamp"), \
    ("cgid", "integer"), \
    ("cuid", "integer"), \
    ("value1", "string"), \
    ("value1num", "float"), \
    ("value1uom", "string"), \
    ("value2", "string"), \
    ("value2num", "float"), \
    ("value2uom", "string"), \
    ("resultstatus", "string"), \
    ("stopped", "string") \
)

def start_chartevents_stream_bronze():
    streamingHDFSToBronze(tableName = 'chartevents', schema = charteventsSchema)

def start_chartevents_stream_silver():
    streamingBronzeToGoldAppendMethod(tableName = 'chartevents', schema = charteventsSchema, partitionedBy = ["subject_id"])

#icustayevents
icustayeventsSchema = ( \
    ("icustay_id", "integer", False), \
	("subject_id", "integer", False), \
	("intime", "timestamp"), \
	("outtime", "timestamp"), \
	("los", "integer"), \
	("first_careunit", "integer"), \
	("last_careunit", "integer") \
)

def start_icustayevents_stream_bronze():
    streamingHDFSToBronze(tableName = 'icustayevents', schema = icustayeventsSchema)

def start_icustayevents_stream_silver():
    streamingBronzeToGoldAppendMethod(tableName = 'icustayevents', schema = icustayeventsSchema, partitionedBy = ["subject_id"])

#ioevents
ioeventsSchema = ( \
    ("subject_id", "integer", False), \
	("icustay_id", "integer"), \
	("itemid", "integer", False), \
	("charttime", "timestamp"), \
	("elemid", "integer"), \
	("altid", "integer"), \
	("realtime", "timestamp"), \
	("cgid", "integer"), \
	("cuid", "integer"), \
	("volume", "float"), \
	("volumeuom", "string"), \
	("unitshung", "string"), \
	("unitshunguom", "string"), \
	("newbottle", "integer"), \
	("stopped", "string"), \
	("estimate", "string") \
)

def start_ioevents_stream_bronze():
    streamingHDFSToBronze(tableName = 'ioevents', schema = ioeventsSchema)

def start_ioevents_stream_silver():
    streamingBronzeToGoldAppendMethod(tableName = 'ioevents', schema = ioeventsSchema, partitionedBy = ["subject_id"])

#labevents
labeventsSchema = ( \
    ("subject_id", "integer", False), \
	("hadm_id", "integer", False), \
	("icustay_id", "integer"), \
	("itemid", "integer", False), \
	("charttime", "timestamp"), \
	("value", "string"), \
	("valuenum", "float"), \
	("flag", "string"), \
	("valueuom", "string") \
)

def start_labevents_stream_bronze():
    streamingHDFSToBronze(tableName = 'labevents', schema = labeventsSchema)

def start_labevents_stream_silver():
    streamingBronzeToGoldAppendMethod(tableName = 'labevents', schema = labeventsSchema, partitionedBy = ["subject_id"])

#medevents
medeventsSchema = ( \
    ("subject_id", "integer", False), \
	("icustay_id", "integer"), \
	("itemid", "integer", False), \
	("charttime", "timestamp"), \
	("elemid", "integer"), \
	("realtime", "timestamp"), \
	("cgid", "integer"), \
	("cuid", "integer"), \
	("volume", "float"), \
	("dose", "float"), \
	("doseuom", "string"), \
	("solutionid", "integer"), \
	("solvolume", "float"), \
	("solunits", "string"), \
	("route", "string"), \
	("stopped", "string") \
)

def start_medevents_stream_bronze():
    streamingHDFSToBronze(tableName = 'medevents', schema = medeventsSchema)

def start_medevents_stream_silver():
    streamingBronzeToGoldAppendMethod(tableName = 'medevents', schema = medeventsSchema, partitionedBy = ["subject_id"])

#init Spark streaming
def init_spark_streaming():
    print('init streaming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", amazonS3param['s3aImpl'])
    hadoop_conf.set("fs.s3a.access.key", amazonS3param['accesskey'])
    hadoop_conf.set("fs.s3a.secret.key", amazonS3param['secretkey'])

    # start_d_patient_stream_bronze()
    # start_d_patient_stream_silver()
    # start_admissions_stream_bronze()
    # start_admissions_stream_silver()
    # start_drgevents_stream_bronze()
    # start_drgevents_stream_silver()
    # start_d_codeditems_stream_bronze()
    # start_d_codeditems_stream_silver()
    # start_demographic_detail_stream_bronze()
    # start_demographic_detail_stream_silver()
    # start_icd9_stream_bronze()
    # start_icd9_stream_silver()
    # start_chartevents_stream_bronze()
    # start_chartevents_stream_silver()
