from app import db

class CacheQuery(db.Document):
    key = db.StringField()
    value = db.ListField()
    def to_json(self):
        return {
                    "key": self.key,
                    "value": self.value
                }