import os
import imp
import fnmatch
import datetime

from peewee import *
from peewee import Node
from termcolor import colored, cprint
from playhouse.migrate import PostgresqlMigrator, Operation, migrate


__version__ = '0.1.0'


class Migration(Model):
    '''This model tracks the migrations that have currently
    been applied, and to determine which migrations to apply
    depending on the direction'''
    name = CharField(max_length=255)
    applied_on = DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (('name',), True),
        )


class MigrationException(Exception):
    pass


class CustomMigrator(PostgresqlMigrator):
    ''' A custom migrator that keeps track of all
    migrations in order to fake them if necessary
    '''
    def __init__(self, database, path, **kwargs):
        PostgresqlMigrator.__init__(self, database)
        # Set options
        self.path = path
        self.direction = kwargs.get('direction', 'up')
        self.migration = kwargs.get('migration', None)
        self.fake = kwargs.get('fake', False)
        self.force = kwargs.get('force', False)

        self.migrations_run = 0
        self.operations = None
        self.last_id = None

        self.initialize()

    def __call__(self, *args):
        self.operations = args

    def raw_query(self, sql):
        return sql


    def initialize(self):
        # If the Migration table doesn't exist, create it
        if not Migration.table_exists():
            Migration.create_table()

        # Determine what the last migration was on this server
        last = Migration.select().order_by(Migration.id.desc()).limit(1).first()
        self.last_id = last.name.split("_")[0] if last else None

        if last:
            cprint("Last run migration %s" % last.name, "magenta")
        else:
            cprint("No migrations have been run yet", "magenta")


    def run(self):
        if self.migration:
            self.apply_migration(self.migration)
        else:
            # Fetch migrations
            migrations = get_migrations(self.path)
            for migration in migrations:
                if not self.force and migration.split("_")[0] <= self.last_id:
                    continue
                self.apply_migration(migration)
                self.migrations_run += 1

        if self.migrations_run or self.force                  :
            cprint("\nNumber of migrations run %d" % self.migrations_run, "magenta")
        else:
            cprint("\nDatabase already upto date!", "magenta")


    def execute_operation(self, op):
        cprint(op, "green")
        if self.fake:
            return False

        if isinstance(op, Operation):
            migrate(op)
        # If raw query, execute it ourselves
        elif isinstance(op, str):
            self.database.execute_sql(op)
        else:
            raise MigrationException("Can't determine type of operation to run")


    def apply_migration(self, migration, **kwargs):
        '''
        Apply a particular migration
        '''
        cprint("\nAttempting to run %s" % migration, "cyan")
        # First check if the migration has already been applied
        exists = Migration.select().where(Migration.name == migration).limit(1).first()
        if exists and self.direction == 'up':
            cprint("This migration has already been run on this server", "red")
            if not self.force:
                return False
            else:
                cprint("Force running this migration again", "yellow")

        # Load the module
        # module_name = "%s.%s" % (migration_path, migration)
        try:
            # module = importlib.import_module(module_name)
            module = imp.load_source(migration, os.path.join(self.path, '%s.py' % migration))
            if not hasattr(module, self.direction):
                raise MigrationException("%s doesn't have %s migration defined" %
                    (migration, self.direction)
                )
            # Actually execute the direction method
            # Note that this doesn't actually run the migrations in the DB yet.
            # This merely collects the steps in the migration, so that if needed
            # we can just fake it and print out the SQL query as well.
            getattr(module, self.direction)(self)
            # Print out each migration and execute it
            for op in self.operations:
                self.execute_operation(op)

            if not self.fake:
                # If successful, create the entry in our log
                if self.direction == 'up' and not exists:
                    Migration.create(name=migration)
                elif self.direction == 'down' and exists:
                    exists.delete_instance()

            cprint("Done", "green")

        except ImportError:
            raise MigrationException("%s migration not found" % migration)


def fake_print(self):
    '''
    This is the overridden __str__ method for Operation
    Recursively prints out the actual query to be executed
    '''
    def _fake_run():
        kwargs = self.kwargs.copy()
        kwargs['generate'] = True
        return _fake_handle_result(
            getattr(self.migrator, self.method)(*self.args, **kwargs)
        )

    def _fake_handle_result(result):
        if isinstance(result, Node):
            sql, params = self._parse_node(result)
            return (sql, params)
        elif isinstance(result, Operation):
            return str(result)
        elif isinstance(result, (list, tuple)):
            return '\n'.join([str(_fake_handle_result(item)) for item in result])

    return str(_fake_run())

# Monkey Patch the Operation to show SQL
setattr(Operation, "__str__", fake_print)


def find_files(directory, pattern):
    ''' Find all files in a directory which matches the pattern '''
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                yield basename


def get_migrations(directory):
    '''
    In the specified directory, get all the files which match the pattern
    000_migration.py
    '''
    files = []
    for fname in find_files(directory, '[0-9]*_*.py'):
        files.append(fname[:-3])
    return sorted(files, key=lambda name: int(name[2:].split("_")[0]))


def migrate(database, path, **kwargs):
    '''
    Execute the migrations. Pass in kwargs
    '''
    options = {
        'direction': kwargs.get('direction', 'up'),
        'fake': kwargs.get('fake', False),
        'force': kwargs.get('force', False),
        'migration': kwargs.get('migration', None),
        'transaction': kwargs.get('transaction', True)
    }

    if not database:
        raise MigrationException("Pass in a valid database")

    if not path or not os.path.isdir(path):
        raise MigrationException("Path to migrations invalid or not readable")

    Migration._meta.database = database
    migrator = CustomMigrator(database, path, **options)
    migrator.run()
