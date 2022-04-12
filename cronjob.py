from cronjobSilverToGold import *
from cronjobGoldToMongoDB import *
from manage_schema import *
from utility import *

#init Schedule jobs
def cron_data_to_mongoDB():
    print('Schedule jobs copy data from Gold to MongoDB...')
    # cache_gold_analysis_patients_by_age()
    # cache_gold_analysis_admissions_and_deied_patients_in_hospital()
    # cache_gold_analysis_get_5_common_diseases_by_month()
    # cache_gold_analysis_diseases_affect_stay_days()
    # cache_gold_analysis_20_common_diseases_clinical_results()
    # cache_gold_analysis_state_affect_total_died_patients()
    # cache_gold_analysis_patients_by_sex()
    # cache_gold_analysis_patients_died_in_hospital()
    # cache_gold_analysis_diseases_clinical_affected_died_patients()
    # cache_gold_analysis_diseases_clinical_by_month()

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
    # cache_mongoDB_analysis_patients_by_age()
    # cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital()
    # cache_mongoDB_analysis_get_5_common_diseases_by_month()
    # cache_mongoDB_analysis_diseases_affect_stay_days()
    # cache_mongoDB_analysis_20_common_diseases_clinical_results()
    # cache_mongoDB_analysis_state_affect_total_died_patients()
    # cache_mongoDB_analysis_patients_by_sex()
    # cache_mongoDB_analysis_patients_died_in_hospital()
    # cache_mongoDB_analysis_diseases_clinical_affected_died_patients()
    # cache_mongoDB_analysis_diseases_clinical_by_month()
    
    # Query all projects
    projects = Project.objects()

    # Query all streams in each project
    for project in projects:
        apis = ApisDefinition.objects(project = project)

        # For each stream
        for api in apis:
            cache_gold_analysis_query(project_name=project.name, key=api.key)