from email.policy import default
from turtle import st
from flask import request, jsonify, make_response
from mongodb import app
from user_defined_class import *
from init_job import *
from manage_user import token_required

###################################################################### PROJECT ##################################################################
# Create project
@app.route('/project', methods =['POST'])
@token_required
def create_project(current_user):
    # creates a dictionary of the form data
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
  
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
    project = Project.objects(user = current_user)

    return jsonify({'body': project})

# Edit project
@app.route('/project/<id>', methods =['PATCH'])
@token_required
def update_project(current_user, id):
    
    print('------')
    print(id)
    print('------')

    jsonData = request.get_json()
    # gets project info
    new_name = jsonData['name']
    new_state = jsonData['state']

    old_state = project.state

    # checking for existing project
    project = Project.objects(id = id, user = current_user).first()
    project.name = new_name
    project.state = new_state

    if new_state == 'STOPPED':
        # STOP ALL STREAMING OF THIS PROJECT
        stop_project_streaming(project=project)
    elif new_state == 'RUNNING' and old_state == 'STOPPED':
        # START ALL STREAMING OF THIS PROJECT
        start_project_streaming(project=project)

    project.save()

    return jsonify({'body': project})

###################################################################### STREAMING ##################################################################
# Create streaming in project
@app.route('/project/<project_id>/streaming', methods =['POST'])
@token_required
def create_streaming(current_user, project_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
    # gets project info
    method = jsonData['method']
    table_name_source = jsonData['table_name_source']
    table_name_sink = jsonData['table_name_sink']
    
    columns = []
    for col in jsonData['columns']:
        columns.append(col)
    
    merge_on = []
    for item in jsonData['merge_on']:
        merge_on.append(item)

    partition_by = []
    for item in jsonData['partition_by']:
        partition_by.append(item)

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        dataset_source = DataSetDefinition.objects(id=jsonData['dataset_source'], project=project).first()
        dataset_sink = DataSetDefinition.objects(id=jsonData['dataset_sink'], project=project).first()
  
        new_streaming = StreammingDefinition(project = project, method = method, merge_on = merge_on, partition_by = partition_by, dataset_source=dataset_source, dataset_sink=dataset_sink, table_name_source=table_name_source, table_name_sink=table_name_sink)

        # Create columns in streaming
        for col in columns:
            new_streaming.columns.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

        new_streaming.save()

        # Start this streaming
        startStream(project=project, stream=new_streaming)

        return jsonify({'body': new_streaming})

    else:  
        return make_response('Project does not exist.', 400)

# Get streamings in project
@app.route('/project/<project_id>/streaming', methods =['GET'])
@token_required
def get_streaming(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()
    print('project: ' + str(project.to_json()))

    if project:
        streamings = StreammingDefinition.objects(project = project)

        print('streamings: ' + str(streamings.to_json()))

        return jsonify({'body': streamings})

    else:  
        return make_response('Project does not exist.', 400)

# Create streaming in project
@app.route('/project/<project_id>/streaming/<streaming_id>', methods =['PATCH'])
@token_required
def update_streaming(current_user, project_id, streaming_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
  
    # gets project info
    method = jsonData['method']
    table_name_source = jsonData['table_name_source']
    table_name_sink = jsonData['table_name_sink']

    columns = []
    for col in jsonData['columns']:
        columns.append(col)

    merge_on = []
    for item in jsonData['merge_on']:
        merge_on.append(item)

    partition_by = []
    for item in jsonData['partition_by']:
        partition_by.append(item)
  
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        old_streaming = StreammingDefinition(id = streaming_id, project = project)
        streaming = StreammingDefinition(id = streaming_id, project = project)

        if streaming:
            streaming.method = method
            streaming.merge_on = merge_on
            streaming.partition_by = partition_by
            streaming.columns = []
            streaming.dataset_source = DataSetDefinition.objects(id=jsonData['dataset_source'], project=project).first()
            streaming.dataset_sink = DataSetDefinition.objects(id=jsonData['dataset_sink'], project=project).first()
            streaming.table_name_sink = table_name_sink
            streaming.table_name_source = table_name_source

            for col in columns:
                streaming.columns.append(ColumnDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

            streaming.save()

            # Stop prev stream, Start new stream (base on name)
            stopStream(project=project, stream=old_streaming)
            startStream(project=project, stream=streaming)

            return jsonify({'body': streaming})
        else:
            return make_response('streaming does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/streaming/<streaming_id>', methods =['DELETE'])
@token_required
def delete_streaming(current_user, project_id, streaming_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

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
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
    # gets project info
    dataset_name = jsonData['dataset_name']
    dataset_type = jsonData['dataset_type']
    folder_name = jsonData['folder_name']

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        new_dataset = DataSetDefinition(project=project, folder_name=folder_name, dataset_type=dataset_type, dataset_name=dataset_name)
        new_dataset.save()

        return jsonify({'body': new_dataset})

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
    project = Project.objects(id = project_id, user = current_user).first()
    print('project: ' + str(project.to_json()))

    if project:
        datasets = DataSetDefinition.objects(project = project)

        print('datasets: ' + str(datasets.to_json()))

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
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
    # gets api info
    key = jsonData['key']
    description = jsonData['description']
    sql = jsonData['sql']

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        new_api = ApisDefinition(project=project, key=key, description=description, sql=sql)
        new_api.save()

        # Create 2 activities: gold + mongo
        ActivitiesDefinition(api=new_api, key=new_api.key, name=project.name + '_gold_' + key, sql=new_api.sql).save()
        ActivitiesDefinition(api=new_api, key=new_api.key, name=project.name + '_mongo_' + key, sql='').save()

        # Run api cache for first time
        cache_gold_analysis_query(project_name=project.name, sql=new_api.sql, key=new_api.key)
        cache_data_to_mongoDB(project_name=project.name, key=new_api.key)

        return jsonify({'body': new_api})

    else:  
        return make_response('Project does not exist.', 400)

# Update api in project
@app.route('/project/<project_id>/apis/<api_id>', methods =['PATCH'])
@token_required
def update_api(current_user, project_id, api_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
    # gets api info
    key = jsonData['key']
    description = jsonData['description']
    sql = jsonData['sql']

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        api = ApisDefinition.objects(id=api_id, project=project).first()

        if api:
            api.key = key
            api.description = description
            api.sql = sql
            api.save()

            # TODO: if change key --> delete old key
            # TODO: run api cache for new key setup

            return jsonify({'body': api})
        else:
            return make_response('Api does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

# Get api in project
@app.route('/project/<project_id>/apis', methods =['GET'])
@token_required
def get_apis(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()
    print('project: ' + str(project.to_json()))

    if project:
        apis = ApisDefinition.objects(project = project)

        print('datasets: ' + str(apis.to_json()))

        return jsonify({'body': apis})

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/apis/<api_id>', methods =['DELETE'])
@token_required
def delete_api(current_user, project_id, api_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:

        # TODO: delete cache key

        ApisDefinition(id = api_id, project = project).delete()

        return make_response('Deleted.', 200)
    else:  
        return make_response('Project does not exist.', 400)



###################################################################### TRIGGERS ##################################################################
# Create trigger in project
@app.route('/project/<project_id>/trigger', methods =['POST'])
@token_required
def create_trigger(current_user, project_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
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
    project = Project.objects(id = project_id, user = current_user).first()

    if project:

        new_trigger = TriggerDefinition(project=project, name=name, status=status, trigger_type=trigger_type, time_interval=time_interval, time_interval_unit=time_interval_unit, cron_day_of_week=cron_day_of_week, cron_hour=cron_hour, cron_minute=cron_minute, activity_ids=activity_ids)

        new_trigger.save()


        # Start trigger
        if new_trigger.status == 'ACTIVE':
            start_trigger(project=project, trigger=new_trigger)
        
        return jsonify({'body': new_trigger})

    else:  
        return make_response('Project does not exist.', 400)

# Update trigger in project
@app.route('/project/<project_id>/trigger/<trigger_id>', methods =['PATCH'])
@token_required
def update_trigger(current_user, project_id, trigger_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
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
    project = Project.objects(id = project_id, user = current_user).first()

    if project:

        old_trigger = TriggerDefinition.objects(id=trigger_id, project=project).first()
        trigger = TriggerDefinition.objects(id=trigger_id, project=project).first()

        trigger.name = name;
        trigger.status = status;
        trigger.trigger_type = trigger_type;
        trigger.time_interval = time_interval;
        trigger.time_interval_unit = time_interval_unit;
        trigger.cron_day_of_week = cron_day_of_week;
        trigger.cron_hour = cron_hour;
        trigger.cron_minute = cron_minute;
        trigger.activity_ids = activity_ids;

        trigger.save()


        # Start trigger
        if trigger.status == 'ACTIVE' and old_trigger.status != 'ACTIVE':
            stop_trigger(project=project, trigger=old_trigger)
            start_trigger(project=project, trigger=trigger)
        elif trigger.status != 'ACTIVE' and old_trigger.status == 'ACTIVE':
            stop_trigger(trigger=old_trigger)
            stop_trigger(trigger=trigger)
        
        return jsonify({'body': trigger})

    else:  
        return make_response('Project does not exist.', 400)

@app.route('/project/<project_id>/trigger/<trigger_id>', methods =['DELETE'])
@token_required
def delete_trigger(current_user, project_id, trigger_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:

        old_trigger = TriggerDefinition(id = trigger_id, project = project).first()
        stop_trigger(trigger=old_trigger)

        TriggerDefinition(id = trigger_id, project = project).delete()

        return make_response('Deleted.', 200)

    else:  
        return make_response('Project does not exist.', 400)

# Get activities in project
@app.route('/project/<project_id>/activities', methods =['GET'])
@token_required
def get_activities(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:

        apis = ApisDefinition.objects(project=project)

        result = []

        for api in apis:
            activities = ActivitiesDefinition.objects(api=api)
            result = result + activities

        return jsonify({'body': result})

    else:  
        return make_response('Project does not exist.', 400)