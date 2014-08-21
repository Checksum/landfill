#!/usr/bin/env python

"""
Peewee migrator

Usage:
  migrate.py (up|down) [--migration=<name>] [--force] [--fake]
  migrate.py (-h | --help)
  migrate.py --version

Options:
  -h --help            Show this screen.
  --version            Show version.
  --migration=<name>   Name of single migration to run
  --force              Force run the migrations
  --fake               Don't actually run the migration, just show the changes to be applied

"""

import os
import landfill

from peewee import *
from docopt import docopt

from models import database_proxy, create_tables

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
MIGRATION_DIR = os.path.join(BASE_DIR, 'migrations')

DATABASE = PostgresqlDatabase('landfill_test', user='postgres')
database_proxy.initialize(DATABASE)



def main():
  args = docopt(__doc__, version='Peewee migrator 0.1')
  # Prepare the options
  options = {
    'direction': 'down' if args.get('down') else 'up',
    'fake': args.get('--fake'),
    'force': args.get('--force'),
    'migration': args.get('--migration')
  }

  create_tables()
  landfill.migrate('postgresql', DATABASE, MIGRATION_DIR, **options)


if __name__ == "__main__":
  main()
