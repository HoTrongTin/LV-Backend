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
from streaming import init_spark_streaming
from cronjob import cron_check_streaming, cron_data_to_Gold, cron_data_to_mongoDB

CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, My name is SMART MEDICAL SYSTEM!'

@app.route('/test-spark2')
def test_spark2():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('start: ' + current_time)
    
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('end: ' + current_time)
    return jsonify({'body': 'None!'})

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

@app.route('/test-spark7', methods=['POST'])
def test_spark7():
    months = request.json['months']
    res = spark.sql("""
select * from
(select drgevents.itemid, description, month(admit_dt) as month, count(*) as num 
from delta.`/delta_MIMIC2/drgevents` as drgevents
join delta.`/delta_MIMIC2/d_codeditems` as d_codeditems
join delta.`/delta_MIMIC2/admissions` as admissions
on drgevents.itemid = d_codeditems.itemid and admissions.hadm_id = drgevents.hadm_id
group by drgevents.itemid, description, month
order by num desc) tmp
where month in ( """ + months + ')').toPandas().head(20)
    res = spark.createDataFrame(res)
    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/get-cached-data')
def get_cached_data():
    key = request.args.get('key')
    data = CacheQuery.objects(key=key).first()
    return jsonify(data.to_json())


# Setup CronJob for checking streaming
schedulercheckstreaming = BackgroundScheduler()
schedulercheckstreaming.add_job(func=cron_check_streaming, trigger="interval", seconds=40)
schedulercheckstreaming.start()

# Setup CronJob for copying data from silver to gold
schedulercopydatatoGold = BackgroundScheduler()
schedulercopydatatoGold.add_job(func=cron_data_to_Gold, trigger="interval", seconds=600)
schedulercopydatatoGold.start()

# Setup CronJob for copying data from gold to mongoDB
schedulercopydatatoGold = BackgroundScheduler()
schedulercopydatatoGold.add_job(func=cron_data_to_mongoDB, trigger="interval", seconds=600)
schedulercopydatatoGold.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: schedulercheckstreaming.shutdown())
atexit.register(lambda: schedulercopydatatoGold.shutdown())
atexit.register(lambda: schedulercheckstreaming.shutdown())

if __name__ == '__main__':
    init_spark_streaming()
    print("List streamming queries: ")
    print(spark.streams.active)
    app.run()