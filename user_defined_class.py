from mongodb import db
from tkinter import CASCADE
from manage_user import *

# Database ORMs

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

class StreammingDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    method = db.StringField(required=True, choices=['APPEND', 'MERGE'])
    columns = db.ListField(db.EmbeddedDocumentField(ColumnDefinition))
    merge_on = db.ListField(db.StringField(min_length=1, max_length=45, required=True))
    partition_by = db.ListField(db.StringField(min_length=1, max_length=45, required=True))

    # Manage DataSetDefinition
    dataset_source = db.ReferenceField(DataSetDefinition, reverse_delete_rule=CASCADE)
    dataset_sink = db.ReferenceField(DataSetDefinition, reverse_delete_rule=CASCADE)
    table_name_source = db.StringField(min_length=1, max_length=45, required=True, unique=True)
    table_name_sink = db.StringField(min_length=1, max_length=45, required=True, unique=True)

    # Manage stream
    bronze_stream_id = db.StringField(default = '')
    gold_stream_id = db.StringField(default = '')
    bronze_stream_name = db.StringField(default = '')
    bronze_stream_status = db.StringField(choices=['ACTIVE', 'IN_ACTIVE'], default = 'IN_ACTIVE')
    gold_stream_name = db.StringField(default = '')
    gold_stream_status = db.StringField(choices=['ACTIVE', 'IN_ACTIVE'], default = 'IN_ACTIVE')

class ApisDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    key = db.StringField(required=True)
    description = db.StringField(default = '')
    sql = db.StringField(default = '')

class TriggerDefinition(db.Document):
    project = db.ReferenceField(Project, reverse_delete_rule=CASCADE)
    trigger_name = db.StringField(required=True)
    trigger_type = db.StringField(required=True, choices=['INTERVAL', 'CRON'])
    trigger_time_interval = db.FloatField(default = 1)
    trigger_time_interval_unit = db.StringField(choices=['SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK'], default = 'HOUR')
    trigger_cron_day_of_week = db.StringField(default = '')
    trigger_cron_hour = db.StringField(default = '')
    trigger_cron_minute = db.StringField(default = '')
    trigger_activities = db.ListField(db.StringField(min_length=1, max_length=45, required=True))