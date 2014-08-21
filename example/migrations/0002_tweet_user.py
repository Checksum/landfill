from example.models import User
from peewee import ForeignKeyField

tweet_user = ForeignKeyField(User, to_field=User.id, db_column="user_id", null=True)
tweet_view = '''
CREATE OR REPLACE VIEW user_tweets_view AS
SELECT users.id, users.name, users.email, tweet.text
FROM users
LEFT JOIN tweet ON tweet.user_id = users.id
ORDER BY tweet.id DESC;
'''

def up(migrator):
	migrator(
		migrator.add_column('tweet', 'user_id', tweet_user),
		migrator.raw_query(tweet_view),
	)

def down(migrator):
	migrator(
		migrator.drop_column('tweet', 'user_id'),
		migrator.raw_query('DROP VIEW user_tweets_view'),
	)
