import os
import re
import sys
import imp
import pwiz
import peewee
import pkgutil
import datetime
import importlib
import playhouse

from peewee import *
from peewee import Node
from playhouse.migrate import *

from types import ModuleType
from cStringIO import StringIO
from termcolor import colored, cprint


__version__ = '0.1.1'


GENERATE_TEMPLATE = '''
from peewee import (BaseModel, {imports})

def up(migrator):
    {create}
    migrator({up})

def down(migrator):
    migrator({down})
'''


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


class CustomMigrator(SchemaMigrator):
    ''' A custom migrator that keeps track of all
    migrations in order to fake them if necessary
    '''
    def __init__(self, database, module, **kwargs):
        SchemaMigrator.__init__(self, database)
        # Set options
        self.module = module
        self.module_name = self.module.__name__
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
            path = os.path.dirname(self.module.__file__)
            migrations = get_migrations(path)
            for migration in migrations:
                if not self.force and migration.split("_")[0] <= self.last_id:
                    continue
                self.apply_migration(migration)
                self.migrations_run += 1

        if self.migrations_run or self.force:
            cprint("\nNumber of migrations run %d" % self.migrations_run, "magenta")
        else:
            cprint("\nDatabase already upto date!", "magenta")


    def execute_operation(self, op):
        cprint(op, "green")
        if self.fake:
            return False

        if isinstance(op, Operation):
            playhouse.migrate.migrate(op)
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
            if not self.force and self.fake:
                return False
            else:
                cprint("Force running this migration again", "yellow")

        # Load the module
        module_name = "%s.%s" % (self.module_name, migration)
        try:
            module = importlib.import_module(module_name)
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


class CustomSqliteMigrator(CustomMigrator, SqliteMigrator):
    pass


class CustomMySQLMigrator(CustomMigrator, MySQLMigrator):
    pass


class CustomPostgresqlMigrator(CustomMigrator, PostgresqlMigrator):
    pass


DATABASE_ALIASES = {
    CustomSqliteMigrator: ['sqlite', 'sqlite3'],
    CustomMySQLMigrator: ['mysql', 'mysqldb'],
    CustomPostgresqlMigrator: ['postgres', 'postgresql'],
}

DATABASE_MAP = dict((value, key)
                    for key in DATABASE_ALIASES
                    for value in DATABASE_ALIASES[key])


class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


class Generator(object):
    def __init__(self, engine, database, models, **kwargs):
        # Tables from the DB, generated by pwiz
        self.db_tables = self.get_pwiz_tables(engine, database)
        self.model_tables = self.get_tables(models)

    def get_tables(self, models):
        return { obj._meta.db_table : obj for obj in
                models.__dict__.itervalues() if
                isinstance(obj, peewee.BaseModel) and
                len(obj._meta.fields) > 1
            }

    def get_pwiz_tables(self, engine, database):
        introspector = pwiz.make_introspector(engine, database.database,
            **database.connect_kwargs
            )
        with Capturing() as code:
            introspector.print_models(None)
        db_model = imp.new_module('db_model')
        exec('\n'.join(code), db_model.__dict__)

        return self.get_tables(db_model)

    def run(self):
        for db_table, obj in self.model_tables.iteritems():
            if not db_table in self.db_tables:
                # This is a new model that we need to create the table for
                continue
            print(obj._meta.fields)


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


def get_migrations(path):
    '''
    In the specified directory, get all the files which match the pattern
    0001_migration.py
    '''
    pattern = re.compile(r"\d+_[\w\d]+")
    modules = [name for _, name, _ in pkgutil.iter_modules([path])
                if pattern.match(name)
            ]

    return sorted(modules, key=lambda name: int(name.split("_")[0]))


def validate_args(engine, database, module):
    if engine not in DATABASE_MAP:
        raise MigrationException('Unrecognized database engine, must be one of: %s' %
            ', '.join(DATABASE_MAP.keys()))

    if not isinstance(database, peewee.Database):
        raise MigrationException("Parameter database has to be a peewee database object")

    if not isinstance(module, ModuleType):
        raise MigrationException("Parameter module has to be a python module")


# Public API
def migrate(engine, database, module, **kwargs):
    '''
    Execute the migrations. Pass in kwargs
    '''
    validate_args(engine, database, module)

    options = {
        'direction': kwargs.get('direction', 'up'),
        'fake': kwargs.get('fake', False),
        'force': kwargs.get('force', False),
        'migration': kwargs.get('migration', None),
        'transaction': kwargs.get('transaction', True),
    }

    Migration._meta.database = database
    migrator = DATABASE_MAP[engine](database, module, **options)
    migrator.run()


def generate(engine, database, models, **kwargs):
    '''
    Generate the migrations by introspecting the db
    '''
    validate_args(engine, database, models)
    generator = Generator(engine, database, models)
    generator.run()
