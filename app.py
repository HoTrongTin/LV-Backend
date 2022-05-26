from flask import jsonify, request
from flask_cors import CORS
import json
import pandas as pd
import numpy as np
import time
import atexit
from build_model_ANNclassifier import build_model
from predict_by_ANNclassifier import predictANN

from appSetup import app, CacheQuery
from sparkSetup import spark
from init_job import *
from manage_user import *
from manage_framework import *
from user_defined_class import *
from utility import parseQuery

from build_models import buildModels
from prediction import predict

CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, My name is SMART MEDICAL SYSTEM!'

#Test: get data from mongoDB
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

#Test: filter data in mongoDB
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

#Test: check query with tranform code sql add path = '/medical/silver/'
@app.route('/queryFormatted', methods=['POST'])
def queryFormatted():
    jsonData = request.get_json()
    # gets project info
    sql = jsonData['sql']
    startTime = time.time()
    print(parseQuery(sql, '/medical/silver/'))
    res = spark.sql(parseQuery(sql, '/medical/silver/'))

    results = res.toJSON().map(lambda j: json.loads(j)).collect()

    return jsonify({'time to execute': time.time() - startTime,
                    'body': results})

#Test: check query
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

#Get all streaming running on spark
@app.route('/get-spark-streaming')
def get_spark_streaming():
    ls = []
    for stream in spark.streams.active:
        ls.append({'id': stream.id,'name': stream.name})
    return jsonify({'body': ls})

#Get all scheduler jobs running
@app.route('/scheduler-jobs')
def get_scheduler_jobs():
    print('+++++++++++++++ JOBS ++++++++++++++')
    scheduler.print_jobs()
    print('+++++++++++++++ ENDD ++++++++++++++')
    return jsonify({'body': 'Get successful!'})

#Build 10 models
@app.route('/build-models')
def build_models():
    startTime = time.time()
    buildModels()
    build_model()
    return jsonify({'body': 'Build models successful!',
                    'time to execute': time.time() - startTime})

#Predict csv file with option model
@app.route('/predict', methods=['POST'])
def predictModels():
    jsonData = request.get_json()
    algorithm = jsonData['algorithm']
    filename = jsonData['filename']
    startTime = time.time()
    if algorithm == 'ANN':
        res = predictANN(prob = 0.28, filename = filename)
        results = res.toJSON().map(lambda j: json.loads(j)).collect()
        return jsonify({'body': results})
    else: 
        res = predict(algorithm, filename = filename)
        results = res.toJSON().map(lambda j: json.loads(j)).collect()
        return jsonify({'body': results})

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    # print("List streamming queries: ")
    # print(spark.streams().active)
    init_project()

    print("List streamming queries: ")
    print(spark.streams.active)

    for stream in spark.streams.active:
        print("+++++ Name +++++")
        print(stream.id)
        print(stream.name)

    app.run()
