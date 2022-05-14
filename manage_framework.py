from email.policy import default
from turtle import st
from urllib import response
from flask import request, jsonify, make_response
from appSetup import app
from user_defined_class import *
from init_job import *
from manage_user import get_parent_from_child, token_required, track_activity

###################################################################### PROJECT ##################################################################
# Create project
@app.route('/project', methods =['POST'])
@token_required
def create_project(current_user):
    # creates a dictionary of the form data
    jsonData = request.get_json()
    
  
    # gets project info
    name = jsonData['name']
    state = jsonData['state']
  
    # checking for existing project
    project = Project.objects(name = name).first()

    if not project:
        # database ORM object
        new_project = Project(
            name = name,
            state = state,
            user = current_user
        )
        # insert new_project
        new_project.save()
  
        return jsonify({'body': new_project})
    else:
        # returns 400 if user already exists
        return make_response('Project already exists.', 400)

# View projects
@app.route('/project', methods =['GET'])
@token_required
def get_project(current_user):
  
    # checking for existing project
    project = Project.objects(user = get_parent_from_child(current_user))

    return jsonify({'body': project})

# Edit project
@app.route('/project/<id>', methods =['PATCH'])
@token_required
def update_project(current_user, id):

    jsonData = request.get_json()
    # gets project info
    new_name = jsonData['name']
    new_state = jsonData['state']

    # checking for existing project
    project = Project.objects(id = id, user = current_user).first()
    old_state = project.state

    project.name = new_name
    project.state = new_state

    project.save()

    if new_state == 'STOPPED' and old_state == 'RUNNING':
        stop_project(project)
    elif new_state == 'RUNNING' and old_state == 'STOPPED':
        start_project(project)
    else:
        stop_project(project)
        start_project(project)

    return jsonify({'body': project})

###################################################################### STREAMING ##################################################################
# Create streaming in project
@app.route('/project/<project_id>/streaming', methods =['POST'])
@token_required
def create_streaming(current_user, project_id):
    jsonData = request.get_json()
  
    # gets project info
    method = jsonData['method']
    table_name_source = jsonData['table_name_source']
    table_name_sink = jsonData['table_name_sink']
    
    name = jsonData['name']
    description = jsonData['description']
    status = jsonData['status']

    query = jsonData['query']

    schemaOnBronze = []
    for col in jsonData['schemaOnBronze']:
        schemaOnBronze.append(col)

    schemaOnSilver = []
    for col in jsonData['schemaOnSilver']:
        schemaOnSilver.append(col)
    
    merge_on = []
    for item in jsonData['merge_on']:
        merge_on.append(item)

    partition_by = []
    for item in jsonData['partition_by']:
        partition_by.append(item)

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        dataset_source = DataSetDefinition.objects(id=jsonData['dataset_source'], project=project).first()
        dataset_sink = DataSetDefinition.objects(id=jsonData['dataset_sink'], project=project).first()

        new_streaming = StreammingDefinition(project = project, name = name, description = description, status = status, method = method, merge_on = merge_on, partition_by = partition_by, dataset_source=dataset_source, dataset_sink=dataset_sink, table_name_source=table_name_source, table_name_sink=table_name_sink, query=query)

        # Create schemaOnBronze in streaming
        for col in schemaOnBronze:
            new_streaming.schemaOnBronze.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

        # Create schemaOnSilver in streaming
        for col in schemaOnSilver:
            new_streaming.schemaOnSilver.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = True))

        new_streaming.save()

        # Start this streaming
        if new_streaming.status == 'ACTIVE':
            startStream(project=project, stream=new_streaming)

        response = {'body': new_streaming}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

# Get streamings in project
@app.route('/project/<project_id>/streaming', methods =['GET'])
@token_required
def get_streaming(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        streamings = StreammingDefinition.objects(project = project)
        return jsonify({'body': streamings})
    else:  
        return make_response('Project does not exist.', 400)

# Get streaming by id in project
@app.route('/project/<project_id>/streaming/<stream_id>', methods =['GET'])
@token_required
def get_streaming_by_id(current_user, project_id, stream_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        streaming = StreammingDefinition.objects(id=stream_id, project = project).first()
        return jsonify({'body': streaming})
    else:  
        return make_response('Project does not exist.', 400)

# Update streaming in project
@app.route('/project/<project_id>/streaming/<streaming_id>', methods =['PATCH'])
@token_required
def update_streaming(current_user, project_id, streaming_id):
    jsonData = request.get_json()
    
  
    # gets project info
    method = jsonData['method']
    table_name_source = jsonData['table_name_source']
    table_name_sink = jsonData['table_name_sink']

    name = jsonData['name']
    description = jsonData['description']
    status = jsonData['status']

    query = jsonData['query']

    schemaOnBronze = []
    for col in jsonData['schemaOnBronze']:
        schemaOnBronze.append(col)
        
    schemaOnSilver = []
    for col in jsonData['schemaOnSilver']:
        schemaOnSilver.append(col)

    merge_on = []
    for item in jsonData['merge_on']:
        merge_on.append(item)

    partition_by = []
    for item in jsonData['partition_by']:
        partition_by.append(item)
  
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        old_streaming = StreammingDefinition.objects(id = streaming_id, project = project).first()
        streaming = StreammingDefinition(id = streaming_id, project = project)

        if streaming:
            streaming.name = name
            streaming.description = description
            streaming.status = status
            streaming.method = method
            streaming.query = query
            streaming.merge_on = merge_on
            streaming.partition_by = partition_by
            streaming.schemaOnBronze = []
            streaming.schemaOnSilver = []
            streaming.dataset_source = DataSetDefinition.objects(id=jsonData['dataset_source'], project=project).first()
            streaming.dataset_sink = DataSetDefinition.objects(id=jsonData['dataset_sink'], project=project).first()
            streaming.table_name_sink = table_name_sink
            streaming.table_name_source = table_name_source

            streaming.bronze_stream_name = "{project_name}-{folder_name}-{table_name}".format(project_name = project.name,folder_name=streaming.dataset_sink.folder_name, table_name = table_name_sink)
            streaming.silver_stream_name = "{project_name}-silver-{table_name}".format(project_name = project.name, table_name = table_name_sink)

            for col in schemaOnBronze:
                streaming.schemaOnBronze.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

            for col in schemaOnSilver:
                streaming.schemaOnSilver.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = True))

            streaming.save()
            
            # Stop prev stream, Start new stream (base on name)
            # Start trigger
            
            if streaming.status != 'ACTIVE' and old_streaming.status == 'ACTIVE':
                
                stopStream(project=project, stream=old_streaming)
            elif streaming.status == 'ACTIVE' and old_streaming.status != 'ACTIVE':
                
                startStream(project=project, stream=streaming)
            else:
                
                stopStream(project=project, stream=old_streaming)
                startStream(project=project, stream=streaming)

            response = {'body': streaming}
            return track_activity(current_user, project, request, response)
        else:
            return make_response('streaming does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/streaming/<streaming_id>', methods =['DELETE'])
@token_required
def delete_streaming(current_user, project_id, streaming_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        old_streaming = StreammingDefinition.objects(id = streaming_id, project = project).first()

        StreammingDefinition(id = streaming_id, project = project).delete()

        # Stop streaming if exist
        stopStream(project=project, stream=old_streaming)

        return make_response('Streaming deleted.', 200)

    else:  
        return make_response('Project does not exist.', 400)


###################################################################### DATASET ##################################################################
# Create dataset in project
@app.route('/project/<project_id>/dataset', methods =['POST'])
@token_required
def create_dataset(current_user, project_id):
    jsonData = request.get_json()
  
    # gets project info
    dataset_name = jsonData['dataset_name']
    dataset_type = jsonData['dataset_type']
    folder_name = jsonData['folder_name']

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        new_dataset = DataSetDefinition(project=project, folder_name=folder_name, dataset_type=dataset_type, dataset_name=dataset_name)
        new_dataset.save()

        response = {'body': new_dataset}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

# Update dataset in project
# @app.route('/project/<project_id>/dataset/<ds_id>', methods =['PATCH'])
# @token_required
# def update_dataset(current_user, project_id, ds_id):
#     jsonData = request.get_json()
#     print('------')
#     print(jsonData)
#     print('------')
#     print(project_id)
  
#     # gets project info
#     dataset_name = jsonData['dataset_name']
#     dataset_type = jsonData['dataset_type']
#     folder_name = jsonData['folder_name']

#     # checking for existing project
#     project = Project.objects(id = project_id, user = current_user).first()

#     if project:
#         dataset = DataSetDefinition.objects(id=ds_id, project=project).first()

#         if dataset:
#             dataset.dataset_name = dataset_name
#             dataset.dataset_type = dataset_type
#             dataset.folder_name = folder_name
#             dataset.save()

#             return jsonify({'body': dataset})
#         else:
#             return make_response('Dataset does not exist.', 400)

#     else:  
#         return make_response('Project does not exist.', 400)

# Get datasets in project
@app.route('/project/<project_id>/dataset', methods =['GET'])
@token_required
def get_dataset(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        datasets = DataSetDefinition.objects(project = project)

        return jsonify({'body': datasets})

    else:  
        return make_response('Project does not exist.', 400)

# @app.route('/project/<project_id>/dataset/<ds_id>', methods =['DELETE'])
# @token_required
# def delete_dataset(current_user, project_id, ds_id):
#     # checking for existing project
#     project = Project.objects(id = project_id, user = current_user).first()

#     if project:

#         # TODO: update streaming that using this dataset
#         # TODO: prevent delete if this dataset is using by any streaming

#         DataSetDefinition(id = ds_id, project = project).delete()
#     else:  
#         return make_response('Project does not exist.', 400)

###################################################################### APIS ##################################################################
# Create api in project
@app.route('/project/<project_id>/apis', methods =['POST'])
@token_required
def create_api(current_user, project_id):
    jsonData = request.get_json()
  
    # gets api info
    key = jsonData['key']
    description = jsonData['description']
    sql = jsonData['sql']

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        new_api = ApisDefinition(project=project, key=key, description=description, sql=sql)
        new_api.save()

        # Create 2 activities: gold + mongo
        ActivitiesDefinition(api=new_api, key=new_api.key, name=project.name + '_gold_' + key, sql=new_api.sql).save()
        ActivitiesDefinition(api=new_api, key=new_api.key, name=project.name + '_mongo_' + key, sql='').save()

        # Run api cache for first time
        cache_gold_analysis_query(project_name=project.name, sql=new_api.sql, key=new_api.key)
        cache_data_to_mongoDB(project_name=project.name, key=new_api.key)

        response = {'body': new_api}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

# Create test api in project
@app.route('/project/<project_id>/apis_test', methods =['POST'])
@token_required
def create_api_test(current_user, project_id):
    jsonData = request.get_json()
  
    # gets api info
    key = jsonData['key']
    description = jsonData['description']
    title = jsonData['title']
    sql = jsonData['sql']
    chartType = jsonData['chartType']
    xLabel = jsonData['xLabel']
    xLabelField = jsonData['xLabelField']
    yLabel = jsonData['yLabel']
    yLabelField = jsonData['yLabelField']
    descField = jsonData['descField']
    numLines = jsonData['numLines']
    tableFields = jsonData['tableFields']

    lineNames = [{'yLabel': yLabel, 'yLabelField': yLabelField}]
    for item in jsonData['lineNames']:
        lineNames.append(item)

    print(jsonData['lineNames'])
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        new_api = ApisDefinition_Test(
            project=project, 
            title = title, 
            key=key, 
            description=description, 
            sql=sql, 
            chartType=chartType, 
            xLabel=xLabel, 
            xLabelField=xLabelField, 
            yLabel=yLabel, 
            yLabelField=yLabelField, 
            descField=descField,
            numLines=numLines,
            lineNames=lineNames,
            tableFields=tableFields
        )
        new_api.lineNames = []
        for line in lineNames:
            new_api.lineNames.append(LineNameDefinition(yLabel = line['yLabel'], yLabelField = line['yLabelField']))

        new_api.save()

        # Create 2 activities: gold + mongo
        ActivitiesDefinition_Test(api=new_api, key=new_api.key, name=project.name + '_test_gold_' + key, sql=new_api.sql).save()
        ActivitiesDefinition_Test(api=new_api, key=new_api.key, name=project.name + '_test_mongo_' + key, sql='').save()

        # Run api cache for first time
        cache_gold_analysis_query(project_name=project.name, sql=new_api.sql, key=new_api.key)
        cache_data_to_mongoDB(project_name=project.name, key=new_api.key)

        response = {'body': new_api}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/testSchema', methods =['POST'])
@token_required
def testSchema(current_user, project_id):
    jsonData = request.get_json()
  
    # gets api info
    tableName = jsonData['tableName']

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        res = spark.sql('DESCRIBE TABLE delta.`/{project_name}/silver/{tableName}`'.format(project_name = project.name, tableName = tableName))
        results = res.toJSON().map(lambda j: json.loads(j)).collect()
        return jsonify({'body': results})

    else:  
        return make_response('Project does not exist.', 400)

# Update api in project
@app.route('/project/<project_id>/apis/<api_id>', methods =['PATCH'])
@token_required
def update_api(current_user, project_id, api_id):
    jsonData = request.get_json()
  
    # gets api info
    key = jsonData['key']
    description = jsonData['description']
    sql = jsonData['sql']

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        api = ApisDefinition_Test.objects(id=api_id, project=project).first()

        if api:
            api.key = key
            api.description = description
            api.sql = sql
            api.save()

            # TODO: if change key --> delete old key
            # TODO: run api cache for new key setup

            response = {'body': api}
            return track_activity(current_user, project, request, response)
        else:
            return make_response('Api does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

# Get api in project
@app.route('/project/<project_id>/apis', methods =['GET'])
@token_required
def get_apis(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        apis = ApisDefinition.objects(project = project)

        return jsonify({'body': apis})

    else:  
        return make_response('Project does not exist.', 400)

# Get test api in project
@app.route('/project/<project_id>/apis_test', methods =['GET'])
@token_required
def get_apis_test(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        apis = ApisDefinition_Test.objects(project = project)
        return jsonify({'body': apis})

    else:  
        return make_response('Project does not exist.', 400)

# Get api test by id
@app.route('/project/<project_id>/apis_test/<api_id>', methods =['GET'])
@token_required
def get_api_test_by_id(current_user, project_id, api_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        api = ApisDefinition_Test.objects(id=api_id, project = project).first()
        data = CacheQuery.objects(key= project.name + '_'+ api.key).first()

        res = {
            "body": {
                "api": api,
                "dataset": data['value']
            }
        }

        return jsonify(res)
    else:  
        return make_response('Project does not exist.', 400)

# Get api by id
@app.route('/project/<project_id>/apis/<api_id>', methods =['GET'])
@token_required
def get_api_by_id(current_user, project_id, api_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        api = ApisDefinition_Test.objects(id=api_id, project = project).first()

        return jsonify({'body': api})

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/apis/<api_id>', methods =['DELETE'])
@token_required
def delete_api(current_user, project_id, api_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        # TODO: delete cache key

        ApisDefinition_Test(id = api_id, project = project).delete()

        response = make_response('Deleted.', 200)
        return track_activity(current_user, project, request, response)
    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/apis_test/<api_id>', methods =['DELETE'])
@token_required
def delete_api_test(current_user, project_id, api_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        # TODO: delete cache key

        ApisDefinition_Test(id = api_id, project = project).delete()

        response = make_response('Deleted.', 200)
        return track_activity(current_user, project, request, response)
    else:  
        return make_response('Project does not exist.', 400)




###################################################################### TRIGGERS ##################################################################
# Create trigger in project
@app.route('/project/<project_id>/trigger', methods =['POST'])
@token_required
def create_trigger(current_user, project_id):
    jsonData = request.get_json()

    # gets api info
    name = jsonData['name']
    status = jsonData['status']
    trigger_type = jsonData['trigger_type']
    time_interval = jsonData['time_interval']
    time_interval_unit = jsonData['time_interval_unit']
    cron_day_of_week = jsonData['cron_day_of_week']
    cron_hour = jsonData['cron_hour']
    cron_minute = jsonData['cron_minute']
    activity_ids = []
    for item in jsonData['activity_ids']:
        activity_ids.append(item)
    

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        new_trigger = TriggerDefinition(project=project, name=name, status=status, trigger_type=trigger_type, time_interval=time_interval, time_interval_unit=time_interval_unit, cron_day_of_week=cron_day_of_week, cron_hour=cron_hour, cron_minute=cron_minute, activity_ids=activity_ids)

        new_trigger.save()


        # Start trigger
        if new_trigger.status == 'ACTIVE':
            start_trigger(project=project, trigger=new_trigger)
        
        response = {'body': new_trigger}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

# Get triggers in project
@app.route('/project/<project_id>/triggers', methods =['GET'])
@token_required
def get_triggers(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        triggers = TriggerDefinition.objects(project = project)

        return jsonify({'body': triggers})

    else:  
        return make_response('Project does not exist.', 400)

# Get trigger by id
@app.route('/project/<project_id>/triggers/<trigger_id>', methods =['GET'])
@token_required
def get_trigger_by_id(current_user, project_id, trigger_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:
        trigger = TriggerDefinition.objects(id=trigger_id, project = project).first()

        return jsonify({'body': trigger})

    else:  
        return make_response('Project does not exist.', 400)

# Update trigger in project
@app.route('/project/<project_id>/trigger/<trigger_id>', methods =['PATCH'])
@token_required
def update_trigger(current_user, project_id, trigger_id):
    jsonData = request.get_json()
  
    # gets api info
    name = jsonData['name']
    status = jsonData['status']
    trigger_type = jsonData['trigger_type']
    time_interval = jsonData['time_interval']
    time_interval_unit = jsonData['time_interval_unit']
    cron_day_of_week = jsonData['cron_day_of_week']
    cron_hour = jsonData['cron_hour']
    cron_minute = jsonData['cron_minute']
    activity_ids = []
    for item in jsonData['activity_ids']:
        activity_ids.append(item)
    

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        old_trigger = TriggerDefinition.objects(id=trigger_id, project=project).first()
        trigger = TriggerDefinition.objects(id=trigger_id, project=project).first()

        trigger.name = name
        trigger.status = status
        trigger.trigger_type = trigger_type
        trigger.time_interval = time_interval
        trigger.time_interval_unit = time_interval_unit
        trigger.cron_day_of_week = cron_day_of_week
        trigger.cron_hour = cron_hour
        trigger.cron_minute = cron_minute
        trigger.activity_ids = activity_ids

        trigger.save()


        # Start trigger
        if trigger.status != 'ACTIVE' and old_trigger.status == 'ACTIVE':
            stop_trigger(trigger=old_trigger)
        elif trigger.status == 'ACTIVE' and old_trigger.status != 'ACTIVE':
            start_trigger(project=project, trigger=trigger)
        else:
            stop_trigger(trigger=old_trigger)
            start_trigger(project=project, trigger=trigger)
        
        response = {'body': "Updated sucessful!"}
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/trigger/<trigger_id>', methods =['DELETE'])
@token_required
def delete_trigger(current_user, project_id, trigger_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        old_trigger = TriggerDefinition(id = trigger_id, project = project).first()
        stop_trigger(trigger=old_trigger)

        TriggerDefinition(id = trigger_id, project = project).delete()

        response = make_response('Deleted.', 200)
        return track_activity(current_user, project, request, response)

    else:  
        return make_response('Project does not exist.', 400)

# Get activities in project
@app.route('/project/<project_id>/activities', methods =['GET'])
@token_required
def get_all_activities(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        apis = ApisDefinition_Test.objects(project=project)

        result = []

        for api in apis:
            activities = ActivitiesDefinition_Test.objects(api=api)
            for act in activities:
                result.append(act)

        return jsonify({'body': result})

    else:  
        return make_response('Project does not exist.', 400)

# Get activities in project
@app.route('/project/<project_id>/trigger/<trigger_id>/activities', methods =['GET'])
@token_required
def get_activities_by_trigger(current_user, project_id, trigger_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = get_parent_from_child(current_user)).first()

    if project:

        apis = ApisDefinition_Test.objects(project=project)

        triggers = TriggerDefinition.objects(project=project)

        found_trigger = TriggerDefinition.objects(id=trigger_id, project=project).first()
        print('found_trigger: ')
        print(found_trigger.id)

        result = []

        for api in apis:
            print('---------------------------------')
            activities = ActivitiesDefinition_Test.objects(api=api)
            for act in activities:
                print('activity_id: ')
                print(act.id)

                is_used = False

                for trigger in triggers:
                    print('trigger_id: ')
                    print(trigger.id)
                    print('trigger.activity_ids')
                    print(trigger.activity_ids)

                    for activity_id in trigger.activity_ids:
                        print(str(activity_id) == str(act.id))
                        if str(activity_id) == str(act.id):
                            print(str(trigger.id) != str(found_trigger.id))
                            if (not found_trigger or (str(trigger.id) != str(found_trigger.id))):
                                is_used = True
                    # if (found_trigger and found_trigger.id == trigger.id): 
                    #     print(found_trigger.id)
                    #     is_used = False
                    print(is_used)
                
                if not is_used:
                    result.append(act)

        return jsonify({'body': result})

    else:  
        return make_response('Project does not exist.', 400)