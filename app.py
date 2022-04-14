from flask import jsonify, request
from flask_cors import CORS
import json
import pandas as pd
import numpy as np
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from mongodb import app, CacheQuery
from sparkSetup import spark
from init_job import init_spark_streaming
from cronjob import cron_data_to_Gold, cron_data_to_mongoDB
from manage_user import *
from user_defined_class import *
from utility import parseQuery

CORS(app)

#Set up scheduler
scheduler = BackgroundScheduler()
scheduler.configure(timezone='Asia/Ho_Chi_Minh')

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
    key = request.args.get('key')
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
    res = spark.sql(sql)

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'time to execute': time.time() - startTime,
                    'body': results})

@app.route('/manual-copy-gold')
def manual_copy_gold():
    cron_data_to_Gold()
    return jsonify({'body': 'Copy successful!'})

@app.route('/manual-copy-mongoDB')
def manual_copy_mongoDB():
    cron_data_to_mongoDB()
    return jsonify({'body': 'Copy successful!'})

@app.route('/manual-stop-scheduler')
def manual_stop_scheduler():
    scheduler.remove_all_jobs()
    return jsonify({'body': 'Stop scheduler successful!'})

@app.route('/manual-start-scheduler')
def manual_start_scheduler():
    scheduler.start()
    return jsonify({'body': 'Stop scheduler successful!'})

# Setup CronJob for checking streaming
# scheduler.add_job(func=cron_check_streaming, trigger="interval", seconds=6000)

# Setup CronJob for copying data from silver to gold
#shceduler run mon to fri on every 0 and 30 minutes of each hour from 6h to 22h
scheduler.add_job(func=cron_data_to_Gold, trigger="cron", minute='0', hour='6-22', day_of_week='mon-fri')

# Setup CronJob for copying data from gold to mongoDB
#shceduler run mon to fri on every 15 and 45 minutes of each hour from 6h to 22h
scheduler.add_job(func=cron_data_to_mongoDB, trigger="cron", minute='5', hour='6-22', day_of_week='mon-fri')

scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    init_spark_streaming()
    print("List streamming queries: ")
    print(spark.streams.active)

    for stream in spark.streams.active:
        print("+++++ Name +++++")
        print(stream.id)
        print(stream.name)

    app.run()