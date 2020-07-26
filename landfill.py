import os
import re
import sys
import imp
import pwiz
import peewee
import inspect
import logging
import pkgutil
import datetime
import importlib
import playhouse

from peewee import *
from peewee import Node
from playhouse.migrate import *

from types import ModuleType
from io import StringIO

__version__ = '0.2.1'


GENERATE_TEMPLATE = '''
from peewee import *
from models import *

{fields}

def up(migrator):
    {up_tables}

    migrator({up_columns}
    )

def down(migrator):
    {down_tables}

    migrator({down_columns}
    )

'''

COLUMN_DEFINITION = {
    'add_column'    : "\n      migrator.add_column('{}', '{}', {}_{})",
    'drop_column'   : "\n      migrator.drop_column('{}', '{}')",
    'create_table'  : "\n    {}.create_table(True)",
    'drop_table'    : "\n    {}.drop_table(True)"
}

COLUMN_DIRECTION = {
    'add_column'    : ('add_column', 'drop_column'),
    'drop_column'   : ('drop_column', 'add_column'),
    'create_table'  : ('create_table', 'drop_table'),
    'drop_table'    : ('drop_table', 'create_table')
}


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class Migration(Model):
    """
    This model tracks the migrations that have currently
    been applied, and to determine which migrations to apply
    depending on the direction
    """
    name = CharField(max_length=255)
    applied_on = DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (('name',), True),
        )


class MigrationException(Exception):
    pass


class CustomMigrator(SchemaMigrator):
    """
    A custom migrator which extends peewee's migrator
    which does a few things:

    1. Keep track of migrations in order to fake them if necessary
    2. Incremental migrations
    """
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
            print("Last run migration %s" % last.name)
        else:
            print("No migrations have been run yet")

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
            print("\nNumber of migrations run %d" % self.migrations_run)
        else:
            print("\nDatabase already upto date!")

    def execute_operation(self, op):
        print(op)
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
        print("\nAttempting to run %s" % migration)
        # First check if the migration has already been applied
        exists = Migration.select().where(Migration.name == migration).limit(1).first()
        if exists and self.direction == 'up':
            print("This migration has already been run on this server")
            if not self.force or self.fake:
                return False
            else:
                print("Force running this migration again")

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

            print("Done")

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
    """
    Util class to capture any code that is printed
    out to stdout. This is necessary to load up the
    table definition that peewee's introspector throws up
    """
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


class Generator(object):
    """
    Automatically generates the list of migrations to be run
    by comparing the states of the model definition and the
    database.

    A lot of this is pretty rudimentary at the moment - Adding
    and removing columns are supported.

    WARNING: Alpha at best.
    """
    def __init__(self, engine, database, models, **kwargs):
        '''
        Terminology:
        py_ = loaded from the python models
        db_ = generated by instrospecting the DB
        '''
        # Python object
        self.py_models = models
        self.db_models = self.get_pwiz_tables(engine, database)
        # Tables from the DB, generated by pwiz
        self.py_tables = self.get_tables(self.py_models)
        self.db_tables = self.get_tables(self.db_models)

        self.source_cache = {}
        # Fields to generate the template
        self.migration_fields = []
        self.up_columns = []
        self.down_columns = []
        self.up_tables = []
        self.down_tables = []

    def get_tables(self, models):
        '''
        Extract all peewee models from the passed in module
        '''
        return { obj._meta.table_name : obj for obj in
                models.__dict__.values() if
                hasattr(obj, '_meta') and
                isinstance(obj, peewee.ModelBase) and
                len(obj._meta.fields) > 1
            }

    def get_pwiz_tables(self, engine, database):
        '''
        Run the pwiz introspector and get the models defined
        in the DB.
        '''
        introspector = pwiz.make_introspector(engine, database.database,
            **database.connect_params)
        out_file = '/tmp/db_models.py'

        with Capturing() as code:
            pwiz.print_models(introspector)
        code = '\n'.join(code)
        # Unfortunately, introspect.getsource doesn't seem to work
        # with dynamically created classes unless it is written out
        # to a file. So write it out to a temporary file
        with open(out_file, 'w') as file_:
            file_.write(code)
        # Load up the DB models as a new module so that we can
        # compare them with those in the model definition
        return imp.load_source('db_models', out_file)

    def run(self):
        for table_name, py_table in self.py_tables.items():
            # If the table exists in the DB, compare its fields
            if table_name in self.db_tables:
                logger.debug("%s already exists in the DB. Checking fields now" % table_name)
                model_set = set(py_table._meta.fields)
                db_set = set(self.db_tables.get(table_name)._meta.fields)
                # Added and deleted columns
                added = model_set - db_set
                deleted = db_set - model_set
                if added:
                    logger.info("Columns added: %s" % added)
                    for column in added:
                        self.generate_definition('add_column', py_table, self.py_models, table_name, column)
                if deleted:
                    logger.info("Columns deleted: %s" % deleted)
                    for column in deleted:
                        self.generate_definition('drop_column', self.db_tables.get(table_name), self.db_models, table_name, column)
            # If new table, create the table
            else:
                logger.info("%s is a new table" % table_name)
                model_class = py_table._meta.model.__name__
                self.up_tables.append(COLUMN_DEFINITION.get('create_table').format(model_class))
                self.down_tables.append(COLUMN_DEFINITION.get('drop_table').format(model_class))


        print(GENERATE_TEMPLATE.format(
            fields='\n'.join(self.migration_fields),
            up_columns=','.join(self.up_columns),
            down_columns=','.join(self.down_columns),
            up_tables=','.join(self.up_tables),
            down_tables=','.join(self.down_tables),
        ))


    def generate_definition(self, _type, table, model, table_name, column):
        field = table._meta.fields.get(column)
        field_type = type(field).__name__
        field_attrs = field.__dict__
        field_name = field_attrs.get('db_column')

        definition = self.get_field(_type, field_attrs, model, table_name, column)
        if definition:
            self.migration_fields.append(definition)
            # Generate the migration statement
            steps = COLUMN_DIRECTION.get(_type)
            self.up_columns.append(COLUMN_DEFINITION.get(steps[0]).format(table_name, field_name, table_name, column))
            self.down_columns.append(COLUMN_DEFINITION.get(steps[1]).format(table_name, field_name, table_name, column))
        else:
            logger.warning("Could not get definition of field %s" % column)

    def get_field(self, _type, field_attrs, model, table_name, column):
        model_name = field_attrs.get('model').__name__
        # Introspect the table definition and search for the field
        # This is done in a very crude way, do it better!
        model_source = self.get_model_source(_type, model, model_name)
        definition = re.search(column + "(.*)", model_source)
        return table_name + '_' + definition.group(0).strip() if definition else None

    def get_model_source(self, _type, model, model_name):
        if not model_name in self.source_cache:
            model_source = inspect.getsource(getattr(model, model_name)).strip()
            self.source_cache[model_name] = model_source
        return self.source_cache.get(model_name, '')


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
