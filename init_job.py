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

#init Spark streaming
def init_spark_streaming():
    print('init streaming')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", amazonS3param['s3aImpl'])
    hadoop_conf.set("fs.s3a.access.key", amazonS3param['accesskey'])
    hadoop_conf.set("fs.s3a.secret.key", amazonS3param['secretkey'])

    # Query all projects
    projects = Project.objects()
    print('Projects: ' + str(projects))

    # Query all streams in each project
    for project in projects:
        start_project_streaming(project)

def start_project_streaming(project):
    streams = StreammingDefinition.objects(project = project)

    # For each stream
    for stream in streams:
        startStream(project=project, stream=stream)

def stop_project_streaming(project):
    streams = StreammingDefinition.objects(project = project)
    # For each stream
    for stream in streams:
        stopStream(project=project, stream=stream)

#init Schedule jobs
def cron_data_to_Gold():
    print('Setup CronJob for copying data from Silver to Gold...')

    # Query all projects
    projects = Project.objects()

    # Query all apis in each project
    for project in projects:
        apis = ApisDefinition.objects(project = project)

        # For each stream
        for api in apis:
            cache_gold_analysis_query(project_name=project.name, sql=api.sql, key=api.key)

def cron_data_to_mongoDB():
    print('Schedule jobs copy data from Gold to MongoDB...')

    # Query all projects
    projects = Project.objects()

    # Query all apis in each project
    for project in projects:
        apis = ApisDefinition.objects(project = project)

        # For each stream
        for api in apis:
            cache_data_to_mongoDB(project_name=project.name, key=api.key)

def init_trigger():
    # Setup CronJob for checking streaming
    # scheduler.add_job(func=cron_check_streaming, trigger="interval", seconds=6000)

    # Setup CronJob for copying data from silver to gold
    #shceduler run mon to fri on every 0 and 30 minutes of each hour from 6h to 22h
    # scheduler.add_job(func=cron_data_to_Gold, trigger="cron", minute='0', hour='6-22', day_of_week='mon-fri')

    # Setup CronJob for copying data from gold to mongoDB
    #shceduler run mon to fri on every 15 and 45 minutes of each hour from 6h to 22h
    # scheduler.add_job(func=cron_data_to_mongoDB, trigger="cron", minute='5', hour='6-22', day_of_week='mon-fri')


    projects = Project.objects()

    # Query all apis in each project
    for project in projects:
        triggers = TriggerDefinition.objects(project = project)

        # For each trigger
        for trigger in triggers:
            #TODO: fix
            activity_ids = trigger.trigger_activities
            if trigger.trigger_type == 'INTERVAL':

                #cal time
                seconds = trigger.trigger_time_interval
                if trigger.trigger_time_interval_unit == 'MINUTE':
                    seconds *= 60
                elif trigger.trigger_time_interval_unit == 'HOUR':
                    seconds *= 60*60
                elif trigger.trigger_time_interval_unit == 'DAY':
                    seconds *= 60*60*24
                elif trigger.trigger_time_interval_unit == 'WEEK':
                    seconds *= 60*60*24*7

                for activity_id in activity_ids:
                    activity = ActivitiesDefinition.objects(id = activity_id)

                    if activity.name[0] == 'g':
                        scheduler.add_job(func=cache_gold_analysis_query(project_name=project.name, sql=activity.sql, key=activity.key), trigger="interval", seconds=seconds)
                    else:
                        scheduler.add_job(func=cache_data_to_mongoDB(project_name=project.name, key=activity.key), trigger="interval", seconds=seconds)
            
            else:
                for activity_id in activity_ids:
                    activity = ActivitiesDefinition.objects(id = activity_id)

                    if activity.name[0] == 'g':
                        scheduler.add_job(func=cache_gold_analysis_query(project_name=project.name, sql=activity.sql, key=activity.key), trigger="cron", minute=trigger.trigger_cron_minute, hour=trigger.trigger_cron_hour, day_of_week=trigger.trigger_cron_day_of_week)
                    else:
                        scheduler.add_job(func=cache_data_to_mongoDB(project_name=project.name, key=activity.key), trigger="cron", minute=trigger.trigger_cron_minute, hour=trigger.trigger_cron_hour, day_of_week=trigger.trigger_cron_day_of_week)

    scheduler.start()