from user_defined_class import *
from utility import *

#init Schedule jobs
def cron_data_to_mongoDB():
    print('Schedule jobs copy data from Gold to MongoDB...')

    # Query all projects
    projects = Project.objects()

    # Query all streams in each project
    for project in projects:
        apis = ApisDefinition.objects(project = project)

        # For each stream
        for api in apis:
            cache_data_to_mongoDB(project_name=project.name, key=api.key)

def cron_data_to_Gold():
    print('Setup CronJob for copying data from Silver to Gold...')

    # Query all projects
    projects = Project.objects()

    # Query all streams in each project
    for project in projects:
        apis = ApisDefinition.objects(project = project)

        # For each stream
        for api in apis:
            cache_gold_analysis_query(project_name=project.name, sql=api.sql, key=api.key)