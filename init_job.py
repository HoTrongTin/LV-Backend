from sparkSetup import spark
from utility import *
import configparser
from user_defined_class import *
from apscheduler.schedulers.background import BackgroundScheduler

#Set up scheduler
scheduler = BackgroundScheduler()
scheduler.configure(timezone='Asia/Ho_Chi_Minh')

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
amazonS3param = config_obj["amazonS3"]

#Start project if Project's state is Running
def init_project():
    #start connection S3
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", amazonS3param['s3aImpl'])
    hadoop_conf.set("fs.s3a.access.key", amazonS3param['accesskey'])
    hadoop_conf.set("fs.s3a.secret.key", amazonS3param['secretkey'])

    #start scheduler
    scheduler.start()

    projects = Project.objects()

    # Query all streams in each project
    for project in projects:
        if project.state == 'RUNNING':
            start_project(project)

#Start project
def start_project(project):
    start_project_streaming(project)
    start_project_trigger(project)

#Stop project
def stop_project(project):
    stop_project_streaming(project)
    stop_project_trigger(project)

#Start Streaming if Streaming's state is ACTIVE
def start_project_streaming(project):
    streams = StreammingDefinition.objects(project = project)

    # For each stream
    for stream in streams:
        if stream.status == 'ACTIVE':
            startStream(project=project, stream=stream)

#Stop Streaming if Streaming's state is ACTIVE
def stop_project_streaming(project):
    streams = StreammingDefinition.objects(project = project)
    # For each stream
    for stream in streams:
        if stream.status == 'ACTIVE':
            stopStream(project=project, stream=stream)

#Start Trigger if Trigger's state is ACTIVE
def start_project_trigger(project):
    triggers = TriggerDefinition.objects(project = project)

    # For each trigger
    for trigger in triggers:
        if trigger.status == 'ACTIVE':
            start_trigger(project, trigger)

#Stop Trigger if Trigger's state is ACTIVE
def stop_project_trigger(project):
    
    triggers = TriggerDefinition.objects(project = project)

    # For each trigger
    for trigger in triggers:
        if trigger.status == 'ACTIVE':
            stop_trigger(trigger)

#Start Trigger
def start_trigger(project, trigger):

    activity_ids = trigger.activity_ids
    if trigger.trigger_type == 'INTERVAL':
        #cal time
        seconds = trigger.time_interval
        if trigger.time_interval_unit == 'MINUTE':
            seconds *= 60
        elif trigger.time_interval_unit == 'HOUR':
            seconds *= 60*60
        elif trigger.time_interval_unit == 'DAY':
            seconds *= 60*60*24
        elif trigger.time_interval_unit == 'WEEK':
            seconds *= 60*60*24*7

        for activity_id in activity_ids:
            activity = ActivitiesDefinition_Test.objects(id = activity_id).first()

            def cache_gold(project, activity):
                cache_gold_analysis_query(project_name=project.name, sql=activity.sql, key=activity.key)
            def cache_mongoDB(project, activity):
                cache_data_to_mongoDB(project_name=project.name, key=activity.key)

            if "_test_gold_" in activity.name:
                scheduler.add_job(id = activity_id, misfire_grace_time=120, func=cache_gold, args=[project, activity], trigger="interval", seconds=seconds)
            elif "_test_mongo_" in activity.name:
                scheduler.add_job(id = activity_id, misfire_grace_time=120, func=cache_mongoDB, args=[project, activity], trigger="interval", seconds=seconds)
            
    else:
        for activity_id in activity_ids:
            activity = ActivitiesDefinition_Test.objects(id = activity_id).first()

            def cache_gold(project, activity):
                cache_gold_analysis_query(project_name=project.name, sql=activity.sql, key=activity.key)
            def cache_mongoDB(project, activity):
                cache_data_to_mongoDB(project_name=project.name, key=activity.key)

            if "_test_gold_" in activity.name:
                scheduler.add_job(id = activity_id, misfire_grace_time=120, func=cache_gold, args=[project, activity], trigger="cron", minute=trigger.cron_minute, hour=trigger.cron_hour, day_of_week=trigger.cron_day_of_week)                
            elif "_test_mongo_" in activity.name:
                scheduler.add_job(id = activity_id, misfire_grace_time=120, func=cache_mongoDB, args=[project, activity], trigger="cron", minute=trigger.cron_minute, hour=trigger.cron_hour, day_of_week=trigger.cron_day_of_week)

#Stop Trigger
def stop_trigger(trigger):
    activity_ids = trigger.activity_ids
    for activity_id in activity_ids:
        scheduler.remove_job(activity_id)