from example.models import User
from peewee import ForeignKeyField

tweet_user = ForeignKeyField(User, to_field=User.id, db_column="user_id", null=True)

def up(migrator):
	migrator(
		migrator.add_column('tweet', 'user_id', tweet_user),
	)

def down(migrator):
	migrator(
		migrator.drop_column('tweet', 'user_id'),
	)
