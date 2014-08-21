from peewee import *

database_proxy = Proxy()

class BaseModel(Model):
  class Meta:
    database = database_proxy


class User(BaseModel):
  name = CharField(null=False, max_length=255)

  class Meta:
    db_table = 'users'


class Tweet(BaseModel):
  text = CharField()

  class Meta:
    db_table = 'tweet'


def create_tables():
  User.create_table(True)
  Tweet.create_table(True)
