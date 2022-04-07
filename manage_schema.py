# flask imports
from enum import Enum
from tkinter import CASCADE
from flask import request, jsonify, make_response
from datetime import datetime, timedelta
from functools import wraps  
from mongodb import app, db
import configparser
import json

from manage_user import *

# Database ORMs

class Project(db.Document):
    name = db.StringField(min_length=6, max_length=200, required=True, unique=True);
    user = db.ReferenceField(User, reverse_delete_rule=CASCADE);


class ModelDefinition(db.EmbeddedDocument):
    name = db.StringField(min_length=1, max_length=45, required=True, unique=True);
    field_type = db.StringField(required=True, choices=['integer', 'float', 'string', 'boolean', 'timestamp']);
    nullable = db.BooleanField(required=True, default=True);

class TableDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    name = db.StringField(min_length=1, max_length=45, required=True, unique=True);
    columns = db.ListField(db.EmbeddedDocumentField(ModelDefinition))


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
    project = Project.objects(id = id, user = current_user).first();
    project.name = name;

    project.save()


    return jsonify({'body': project})

#TODO: Create table in project
@app.route('/project/<project_id>/table', methods =['POST'])
@token_required
def create_table(current_user, project_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
    print(project_id)
  
    # gets project info
    name = jsonData['name']
    # columns = List(jsonData['columns'])
    columns = []
    for col in jsonData['columns']:
        columns.append(col)
  
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        new_table = TableDefinition(project = project, name = name);

        # Create columns in table
        for col in columns:
            new_table.columns.append(ModelDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

        new_table.save()

        return jsonify({'body': new_table})

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Get tables in project
@app.route('/project/<project_id>/table', methods =['GET'])
@token_required
def get_table(current_user, project_id):

    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()
    print('project: ' + str(project.to_json()));

    if project:
        tables = TableDefinition(project = project);

        print('tables: ' + str(tables.to_json()));

        return jsonify({'body': tables})

    else:  
        return make_response('Project does not exist.', 400)

#TODO: Create table in project
@app.route('/project/<project_id>/table/<table_id>', methods =['PATCH'])
@token_required
def update_table(current_user, project_id, table_id):
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
  
    # gets project info
    name = jsonData['name']
    columns = []
    for col in jsonData['columns']:
        columns.append(col)
  
    # checking for existing project
    project = Project.objects(id = project_id, user = current_user).first()

    if project:
        table = TableDefinition(id = table_id, project = project);

        if table:
            # Create columns in table
            
            table.name = name;
            table.columns = [];

            for col in columns:
                table.columns.append(ModelDefinition(name = col['name'], field_type = col['field_type'], nullable = col['nullable']))

            table.save()

            return jsonify({'body': table})
        else:
            return make_response('Table does not exist.', 400)

    else:  
        return make_response('Project does not exist.', 400)