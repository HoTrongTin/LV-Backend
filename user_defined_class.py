from email.policy import default
from appSetup import db
from tkinter import CASCADE
from enum import Enum

# class UserRole(str, Enum):
#     DOCTOR = 'doctor'
#     PATIENT = 'patient'
  
# Database ORMs
class User(db.Document):
    email = db.EmailField(min_length=6, max_length=200, required=True, unique=True)
    password = db.StringField(required=True)
    name = db.StringField(required=True)
    role = db.StringField(choices=['DOCTOR', 'PATIENT', 'ADMIN', 'ASSISTANT'])
    parentID = db.StringField(default='')

class Project(db.Document):
    name = db.StringField(min_length=6, max_length=200, required=True, unique=True)
    state = db.StringField(choices=['RUNNING', 'STOPPED'], default = 'RUNNING')
    user = db.ReferenceField(User, reverse_delete_rule=CASCADE)

class DataSetDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    dataset_name = db.StringField(min_length=1, max_length=45, required=True)
    dataset_type = db.StringField(required=True, choices=['S3', 'HDFS', 'KAFKA'])
    folder_name = db.StringField(default = '')


class ColumnDefinition(db.EmbeddedDocument):
    name = db.StringField(min_length=1, max_length=45, required=True)
    field_type = db.StringField(required=True, choices=['integer', 'float', 'string', 'boolean', 'timestamp'])
    nullable = db.BooleanField(required=True, default=True)

class LineNameDefinition(db.EmbeddedDocument):
    yLabel = db.StringField(default = '')
    yLabelField = db.StringField(default = '')

class StreammingDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    method = db.StringField(required=True, choices=['APPEND', 'MERGE'])
    schemaOnBronze = db.ListField(db.EmbeddedDocumentField(ColumnDefinition))
    schemaOnSilver = db.ListField(db.EmbeddedDocumentField(ColumnDefinition))
    query = db.StringField(required=True)

    merge_on = db.ListField(db.StringField(min_length=1, max_length=45, required=True))
    partition_by = db.ListField(db.StringField(min_length=1, max_length=45, required=True))
    name = db.StringField(required=True)
    description = db.StringField(default = '')
    status = db.StringField(choices=['ACTIVE', 'IN_ACTIVE'], default = 'IN_ACTIVE')

    # Manage DataSetDefinition
    dataset_source = db.ReferenceField(DataSetDefinition, reverse_delete_rule=CASCADE)
    dataset_sink = db.ReferenceField(DataSetDefinition, reverse_delete_rule=CASCADE)
    table_name_source = db.StringField(min_length=1, max_length=45, required=True, unique=True)
    table_name_sink = db.StringField(min_length=1, max_length=45, required=True, unique=True)

class ApisDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    key = db.StringField(required=True)
    description = db.StringField(default = '')
    sql = db.StringField(default = '')

class ApisDefinition_Test(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    key = db.StringField(required=True)
    title = db.StringField(required=True)
    description = db.StringField(default = '')
    sql = db.StringField(default = '')
    chartType = db.StringField(choices=['BAR', 'PIE', 'LINE', 'TABLE'], required=True)
    xLabel = db.StringField(default = '')
    xLabelField = db.StringField(default = '')
    yLabel = db.StringField(default = '')
    yLabelField = db.StringField(default = '')
    descField = db.StringField(default = '')
    numLines = db.FloatField(default = 0)
    lineNames = db.ListField(db.EmbeddedDocumentField(LineNameDefinition))
    tableFields = db.ListField(db.StringField())

class ActivitiesDefinition(db.Document):
    api = db.ReferenceField(ApisDefinition, reverse_delete_rule=CASCADE)
    key = db.StringField(required=True)
    name = db.StringField(default = '')
    sql = db.StringField(default = '')

class ActivitiesDefinition_Test(db.Document):
    api = db.ReferenceField(ApisDefinition_Test, reverse_delete_rule=CASCADE)
    key = db.StringField(required=True)
    name = db.StringField(default = '')
    sql = db.StringField(default = '')

class TriggerDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    name = db.StringField(required=True)
    status = db.StringField(choices=['ACTIVE', 'IN_ACTIVE'], default = 'ACTIVE')
    trigger_type = db.StringField(required=True, choices=['INTERVAL', 'CRON'])
    time_interval = db.FloatField(default = 1)
    time_interval_unit = db.StringField(choices=['SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK'], default = 'HOUR')
    cron_day_of_week = db.StringField(default = '')
    cron_hour = db.StringField(default = '')
    cron_minute = db.StringField(default = '')
    activity_ids = db.ListField(db.StringField())

class ActivityLog(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    actor = db.ReferenceField(User, reverse_delete_rule=CASCADE)
    api_path = db.StringField()
    body = db.StringField()
    response = db.StringField()