from email.policy import default
from flask import request, jsonify, make_response
from mongodb import app
from user_defined_class import *

#TODO: Create project
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
  
    # checking for existing project
    project = Project.objects(name = name).first()

    if not project:
        # database ORM object
        new_project = Project(
            name = name,
            user = current_user
        )
        # insert new_project
        new_project.save()
  
        return jsonify({'body': new_project})
    else:
        # returns 400 if user already exists
        return make_response('Project already exists.', 400)

#TODO: View projects
@app.route('/project', methods =['GET'])
@token_required
def get_project(current_user):
  
    # checking for existing project
    project = Project.objects(user = current_user)

    return jsonify({'body': project})

#TODO: Edit project
@app.route('/project/<id>', methods =['PATCH'])
@token_required
def update_project(current_user, id):
    
    print('------')
    print(id)
    print('------')

    jsonData = request.get_json()
    # gets project info
    name = jsonData['name']

    # checking for existing project
    project = Project.objects(id = id, user = current_user).first()
    project.name = name

    project.save()


    return jsonify({'body': project})

#TODO: Create streaming in project
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

        return jsonify({'body': new_streaming})

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Get streamings in project
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

#TODO: Create streaming in project
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
        StreammingDefinition(id = streaming_id, project = project).delete()
    else:  
        return make_response('Project does not exist.', 400)


#####################################################################################################
#TODO: Create dataset in project
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

#TODO: Update dataset in project
@app.route('/project/<project_id>/dataset/<ds_id>', methods =['PATCH'])
@token_required
def update_dataset(current_user, project_id, ds_id):
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
        dataset = DataSetDefinition.objects(id=ds_id, project=project).first()

        if dataset:
            dataset.dataset_name = dataset_name
            dataset.dataset_type = dataset_type
            dataset.folder_name = folder_name
            dataset.save()

            return jsonify({'body': dataset})
        else:
            return make_response('Dataset does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Get datasets in project
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

@app.route('/project/<project_id>/dataset/<ds_id>', methods =['DELETE'])
@token_required
def delete_dataset(current_user, project_id, ds_id):
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        DataSetDefinition(id = ds_id, project = project).delete()
    else:  
        return make_response('Project does not exist.', 400)

#####################################################################################################
#TODO: Create api in project
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

        return jsonify({'body': new_api})

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Update api in project
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

            return jsonify({'body': api})
        else:
            return make_response('Api does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Get api in project
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
        ApisDefinition(id = api_id, project = project).delete()
    else:  
        return make_response('Project does not exist.', 400)