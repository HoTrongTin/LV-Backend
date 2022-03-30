import configparser
from flask_mongoengine import MongoEngine
from app import app

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
MongoDBparam = config_obj["MONGODB"]

# Setup MongoDB
app.config['MONGODB_SETTINGS'] = {
    'db': MongoDBparam['db'],
    'host': MongoDBparam['host'],
    'port': int(MongoDBparam['port'])
}
db = MongoEngine()
db.init_app(app)

class CacheQuery(db.Document):
    key = db.StringField()
    value = db.ListField()
    def to_json(self):
        return {
                    "key": self.key,
                    "value": self.value
                }