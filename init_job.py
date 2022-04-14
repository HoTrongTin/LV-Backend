from sparkSetup import spark
from utility import *
import configparser
from user_defined_class import *

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
        streams = StreammingDefinition.objects(project = project)

        # For each stream
        for stream in streams:
            startStream(project=project, stream=stream)


    
