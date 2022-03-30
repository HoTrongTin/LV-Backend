from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import pandas as pd
import numpy as np
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from mongodb import CacheQuery
from sparkSetup import *
from streaming import *
from cronjob import cron_cache_query

app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/test-json')
def test_json():
    return jsonify({
        'name': 'Tin Ho Trongg',
        'sex': 'No',
        'age': '1000'
    })

@app.route('/test-spark')
def test_spark():
    df = spark.sql("""
    select * from delta.`/delta_MIMIC2/d_patients`
    where subject_id = '7391'
    """)
    df
    df.show()
    results = df.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify({'body': results})

@app.route('/test-spark2')
def test_spark2():
    # start test_spark2
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('start test_spark2: ' + current_time)
    # --
    df = spark.sql("""
    select * from delta.`/delta_MIMIC2/d_patients`
    where subject_id = '1000'
    """)

    results = df.toJSON().map(lambda j: json.loads(j)).collect()

    # end test_spark2
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print('end test_spark2: ' + current_time)
    # --

    return jsonify({'body': results})

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

    # --

    return jsonify({'body': results})

@app.route('/test-spark4')
def test_spark4():
    res = spark.sql("""
    select year(admit_dt) year, count(*) num from delta.`/delta_MIMIC2/admissions` as admissions
    where year(admit_dt) >= 2005 and year(admit_dt) <= 2015
    group by year(admit_dt)
    order by year(admit_dt)
    """).toPandas()

    dfdied = spark.sql("""
    select year(dod) year, count(*) num from delta.`/delta_MIMIC2/d_patients` as d_patients
    where year(dod) >= 2005 and year(dod) <= 2015 and hospital_expire_flg = 'Y'
    group by year(dod)
    order by year(dod)
    """).toPandas()
    
    res['numDied'] = dfdied['num']
    res = spark.createDataFrame(res)

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/test-spark5')
def test_spark5():
    res = spark.sql("""
    select period, count(*) num from
    (select 
    case 
        WHEN age < 2 THEN 'Infancy' 
        WHEN age >= 2 and age < 6 THEN 'Early Childhood' 
        WHEN age >= 6 and age < 12 THEN 'Later Childhood'
        WHEN age >= 12 and age < 20 THEN 'Adolescence'
        WHEN age >= 20 and age < 40 THEN 'Young adult'
        WHEN age >= 40 and age < 60 THEN 'Middle-aged'
        ELSE 'Senior Citizen' 
    END as period from
    (select extract(day from dod - dob)/365 as age from delta.`/delta_MIMIC2/d_patients`) age) period
    group by period
    order by num desc
    """)

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/test-spark6')
def test_spark6():
    res = spark.sql("""
select drgevents.itemid,description, count(*) as numCases  from delta.`/delta_MIMIC2/drgevents` as drgevents 
join delta.`/delta_MIMIC2/d_codeditems` as d_codeditems 
on drgevents.itemid = d_codeditems.itemid
group by drgevents.itemid, description, type
order by numCases desc
""").toPandas().head(20)
    res = spark.createDataFrame(res)
    results = res.toJSON().map(lambda j: json.loads(j)).collect()

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

@app.route('/test-spark8')
def test_spark8():
    res = spark.sql("""
select * from 
(select ROW_NUMBER() OVER(PARTITION BY month ORDER BY num desc) 
    AS ROW_NUMBER, * from
(select drgevents.itemid, description, month(admit_dt) as month, count(*) as num 
from delta.`/delta_MIMIC2/drgevents` as drgevents
join delta.`/delta_MIMIC2/d_codeditems` as d_codeditems
join delta.`/delta_MIMIC2/admissions` as admissions
on drgevents.itemid = d_codeditems.itemid and admissions.hadm_id = drgevents.hadm_id
group by drgevents.itemid, description, month
order by num desc) tmp) tmp1
where ROW_NUMBER < 6""")

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/test-spark9')
def test_spark9():
    res = spark.sql("""
select itemid, description, avg(stay_days) as avgStayDays from
(select EXTRACT( DAY FROM (disch_dt - admit_dt)) as stay_days, description, drgevents.itemid 
from delta.`/delta_MIMIC2/drgevents` as drgevents 
join delta.`/delta_MIMIC2/admissions` as admissions
join delta.`/delta_MIMIC2/d_codeditems` as d_codeditems
on d_codeditems.itemid = drgevents.itemid 
and drgevents.hadm_id = admissions.hadm_id) tmp
group by itemid, description
order by avg(stay_days) desc
""")

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/test-streamming-1')
def test_streamming_1():
    res = spark.read.format("delta").load("/tmp/admissions")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

@app.route('/test-streamming-2')
def test_streamming_2():
    res = spark.read.format("delta").load("/tmp/d_patients")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'body': results})

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

# Setup CronJob
scheduler = BackgroundScheduler()
scheduler.add_job(func=cron_cache_query, trigger="interval", seconds=60)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    init_spark_streaming()
    print("List streamming queries: ")
    print(spark.streams.active)
    app.run()