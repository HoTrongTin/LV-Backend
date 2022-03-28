from flask import Flask, jsonify
import pyspark
from delta import *
import json
import pandas as pd
import numpy as np

app = Flask(__name__)

builder = pyspark.sql.SparkSession.builder.appName("pyspark-notebook") \
    .master("spark://10.1.8.101:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/test-json')
def test_json():
    return jsonify({
        'name': 'Tín Hồ Trọng',
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
    df = spark.sql("""
    select * from delta.`/delta_MIMIC2/d_patients`
    where subject_id = '1000'
    """)
    df
    df.show()
    results = df.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify({'body': results})

@app.route('/test-spark3/<id>')
def test_spark3(id):
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
    return jsonify({'body': results})

if __name__ == '__main__':
    app.run()
