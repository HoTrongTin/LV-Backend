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

    return jsonify({'body': project.to_json()})

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
    project = Project.objects(id = id, user = current_user).update(name = name).save();

    return jsonify({'body': project.to_json()})