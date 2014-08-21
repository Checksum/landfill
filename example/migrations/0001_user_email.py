from peewee import CharField

def up(migrator):
	migrator(
		migrator.add_column('users', 'email', CharField(null=False, default='')),
		migrator.add_index('users', ('email',), True),
	)
