from flask import jsonify, request
from flask_cors import CORS
import json
import pandas as pd
import numpy as np
import time
import atexit
from build_model_CNNclassifier import build_model
from predict_by_CNNclassifier import predict

from appSetup import app, CacheQuery
from sparkSetup import spark
from init_job import *
from manage_user import *
from manage_framework import *
from user_defined_class import *
from utility import parseQuery

from cronjobSilverToGold import *
from cronjobGoldToMongoDB import *

CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, My name is SMART MEDICAL SYSTEM!'

@app.route('/test-spark3/<id>')
def test_spark3(id):
    # start
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('start-' + id + ': ' + current_time)
    # --

    df = spark.sql("""
    select icd9.code, d_codeditems.itemid
    from delta.`/delta_MIMIC2/icd9` icd9
    join delta.`/delta_MIMIC2/drgevents` drgevents
    join delta.`/delta_MIMIC2/d_codeditems` d_codeditems
    on icd9.hadm_id = drgevents.hadm_id and d_codeditems.itemid = drgevents.itemid
    """).toPandas()

    res = []
    totalCases = df[(df.itemid==id)].shape[0]
    for code in set(df[(df.itemid==id)].code):
        res.append([code, df[(df.itemid==id) & (df.code==code)].shape[0] / totalCases])

    res = pd.DataFrame(np.array(res), columns = ['code', 'ratio']).sort_values(by='ratio',ascending=False).head(10)
    res = spark.createDataFrame(res)

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    # end
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('end-' + id + ': ' + current_time)

    return jsonify({'body': results})

@app.route('/test-chartevents/<subject_id>')
def test_chartevents(subject_id):
    startTime = time.time()
    res = spark.sql("""
    select * from delta.`/delta_MIMIC2/chartevents` as chartevents
    where subject_id = """ + subject_id)
    print("Execution time: " + str(time.time() - startTime))

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/project/<project_id>/get-cached-data')
@token_required
def get_cached_data(current_user, project_id):

    project = Project.objects(id=project_id, user=current_user).first()

    if project:
        key = request.args.get('key')
        data = CacheQuery.objects(key= project.name + '_'+ key).first()
        return jsonify(data.to_json())

    else:
        return make_response('Project does not exist.', 400)

@app.route('/analysis-clinical-diseases-by-month', methods=['POST'])
def test():
    months = request.json['months']
    key = 'medical_' + request.args.get('key')
    data = CacheQuery.objects(key=key).first().value
    res = []
    for item in data:
        if item['month'] in months:
            res.append(item)
    return jsonify({'body': res})

@app.route('/queryFormatted', methods=['POST'])
def queryFormatted():
    jsonData = request.get_json()
    # gets project info
    sql = jsonData['sql']
    startTime = time.time()
    res = spark.sql(parseQuery(sql, '/medical/silver/'))

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'time to execute': time.time() - startTime,
                    'body': results})

@app.route('/query', methods=['POST'])
def query():
    jsonData = request.get_json()
    # gets project info
    sql = jsonData['sql']
    startTime = time.time()
    print(startTime)
    res = spark.sql(sql)

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'time to execute': time.time() - startTime,
                    'body': results})

@app.route('/manual-copy-gold')
def manual_copy_gold():
    gold_analyze_admissions_and_deied_patients_in_hospital()
    gold_analyze_state_affect_total_died_patients()
    gold_analyze_patients_died_in_hospital()
    return jsonify({'body': 'Copy successful!'})

@app.route('/manual-copy-mongoDB')
def manual_copy_mongoDB():
    cache_mongoDB_analyze_admissions_and_deied_patients_in_hospital()
    cache_mongoDB_analyze_state_affect_total_died_patients()
    cache_mongoDB_analyze_patients_died_in_hospital()
    return jsonify({'body': 'Copy successful!'})

@app.route('/manual-stop-scheduler')
def manual_stop_scheduler():
    scheduler.remove_all_jobs()
    return jsonify({'body': 'Stop scheduler successful!'})

@app.route('/manual-start-scheduler')
def manual_start_scheduler():
    scheduler.start()
    return jsonify({'body': 'Stop scheduler successful!'})

@app.route('/get-spark-streaming')
def get_spark_streaming():
    ls = []
    for stream in spark.streams.active:
        ls.append({'id': stream.id,'name': stream.name})
    return jsonify({'body': ls})

@app.route('/scheduler-jobs')
def get_scheduler_jobs():
    print('+++++++++++++++ JOBS ++++++++++++++')
    scheduler.print_jobs()
    print('+++++++++++++++ ENDD ++++++++++++++')
    return jsonify({'body': '+++++++++++++++ ENDD ++++++++++++++'})

@app.route('/build-model-CNNclassifier')
def build_model_CNNclassifier():
    build_model()
    return jsonify({'body': 'Build model CNNclassifier successful!'})

@app.route('/predict-by-CNNclassifier')
def predict_by_CNNclassifier():
    df = predict(prob = 0.28)
    res = spark.createDataFrame(df)
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify({'body': results})

<<<<<<< HEAD
@app.route('/test-cache-query')
def test_cache_query():
    key = request.args.get('key')
    print(key)
    if key == 'cache_test_streaming_1':
        data = CacheQuery.objects(key=key).first()
        print('--Found Mongo Data--')
        print(data)
        return jsonify(data.to_json())
    else:
        return jsonify({'error': 'data not found'})

#streamming
def init_spark_streamming():
    print('init streamming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "AKIASIV2BBOBY7OLXVET")
    hadoop_conf.set("fs.s3a.secret.key", "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")

    start_d_patient_stream()
    start_admission_stream()


def start_d_patient_stream():
    # Define schema of the csv
    d_patientsSchema = StructType() \
        .add("subject_id", "string") \
        .add("sex", "string") \
        .add("dob", "string") \
        .add("dod", "string") \
        .add("hospital_expire_flg", "string")

    dfD_patients = spark.readStream.option("sep", ",").option("header", "true").schema(d_patientsSchema).csv("s3a://sister-team/spark-streamming/d_patients")

    # dfD_patients = spark.readStream.option("sep", ",").option("inferSchema" , "true").option("header", "true").csv("s3a://sister-team/spark-streamming/d_patients")
    dfD_patients \
    .writeStream \
    .format('delta') \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/d_patients/checkpointD_patients") \
    .start("/tmp/d_patients")

def start_admission_stream():
    admissionsSchema = StructType() \
    .add("hadm_id", "string") \
    .add("subject_id", "string") \
    .add("admit_dt", "string") \
    .add("disch_dt", "string")

    dfAdmissions = spark.readStream.option("sep", ",").option("header", "true").schema(admissionsSchema).csv("s3a://sister-team/spark-streamming/admissions")
    dfAdmissions \
    .writeStream \
    .format('delta') \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/admissions/checkpointAdmissions") \
    .start("/tmp/admissions")

#Create Silver table
# @app.route('/create-silver-table')
# def create_silver_table():
#     #d_patients
#     spark.sql("CREATE TABLE silver_d_patients (subject_id string, sex string, dob timestamp, dod timestamp, hospital_expire_flg string, Date_Time timestamp) USING DELTA LOCATION '/medical/silver/d_patients'")

#Schedule jobs
def cron_cache_query():
    print('Cron job running...')
    cache_test_streaming_1()
    # merge_silver_d_patients()

# def merge_silver_d_patients():
#     spark.sql("""
# MERGE INTO delta.`/medical/silver/d_patients` silver_d_patients
# USING (select * from delta.`/medical/bronze/d_patients`
# where Date_Time > (select CASE WHEN max(Date_Time) is not NULL THEN max(Date_Time) ELSE '2000-01-01 00:00:00' END from delta.`/medical/bronze/d_patients`)
# ) updates
# ON silver_d_patients.subject_id = updates.subject_id
# WHEN MATCHED THEN
#   UPDATE SET *
# WHEN NOT MATCHED
#   THEN INSERT *
# """)

def cache_test_streaming_1():
    res = spark.read.format("delta").load("/tmp/admissions")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    
    data = CacheQuery(key='cache_test_streaming_1',value=results)
    data.save()

# Setup CronJob
scheduler = BackgroundScheduler()
scheduler.add_job(func=cron_cache_query, trigger="interval", seconds=60)
scheduler.start()
=======
#init trigger by schedule
>>>>>>> phuc

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
<<<<<<< HEAD
    init_spark_streamming()
<<<<<<< HEAD
    # print("List streamming queries: ")
    # print(spark.streams().active)
=======
    print("List streamming queries: ")
    print(spark.streams.active)
>>>>>>> 31664328f8f833849074155d0b74fcc6a9019455
    app.run()
=======
    init_project()
    print("List streamming queries: ")
    print(spark.streams.active)

    for stream in spark.streams.active:
        print("+++++ Name +++++")
        print(stream.id)
        print(stream.name)

    app.run()
>>>>>>> phuc
