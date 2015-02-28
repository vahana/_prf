import os
import sys
import logging

from argparse import ArgumentParser
from pyramid.paster import get_appsettings
import sqlalchemy as sa
from sqlalchemy import engine_from_config
from sqlalchemy_utils.functions import database_exists, create_database, \
                                       drop_database

from alembic.config import Config
from alembic import command

from prf.scripts.common import package_name

log = logging.getLogger(__name__)


class InitDB(object):

    def __init__(self, argv):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument('-c', help='Config ini file', required=True)
        parser.add_argument('--drop', help='Drop DB', action='store_true')

        self.options = parser.parse_args()
        self.config = self.options.c
        self.package_name = package_name(argv)
        self.settings = get_appsettings(self.config, self.package_name)
        self.package_name = package_name(argv)
        self.db = engine_from_config(self.settings, 'db.')

    def run(self):

        if self.options.drop:
            if database_exists(self.db.url):
                in_ = raw_input('Are you sure you want to drop?(y/n): ')
                if in_ == 'y':
                    drop_database(self.db.url)
                else:
                    print 'Canceled'
                    return
            else:
                print 'Nothing to drop'

        if database_exists(self.db.url):
            print '%s db exists already. Use --drop to drop and recreate' % self.db.url
            return

        create_database(self.db.url)

        self.create_tables()

        # load the Alembic configuration and generate the
        # version table, "stamping" it with the most recent rev:
        alembic_cfg = Config(self.config)
        command.stamp(alembic_cfg, 'head')

        print 'DB initialized' + ((' (new)' if self.options.drop else ''))
