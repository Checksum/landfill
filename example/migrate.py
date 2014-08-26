#!/usr/bin/env python

"""
Peewee migrator

Usage:
  migrate.py generate
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

import sys
sys.path.insert(1, '../')

import os
import landfill

import models
import migrations

from peewee import *
from docopt import docopt

from models import database_proxy, create_tables


DATABASE = PostgresqlDatabase('landfill_test', user='postgres')
database_proxy.initialize(DATABASE)


def main():
  args = docopt(__doc__, version='Peewee migrator 0.2')

  if args.get('generate'):
    landfill.generate('postgresql', DATABASE, models)

  else:
    options = {
      'direction': 'down' if args.get('down') else 'up',
      'fake': args.get('--fake'),
      'force': args.get('--force'),
      'migration': args.get('--migration')
    }
    landfill.migrate('postgresql', DATABASE, migrations, **options)


if __name__ == "__main__":
  main()
