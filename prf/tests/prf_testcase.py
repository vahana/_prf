import os
import unittest
from pyramid.config import Configurator
from pyramid.paster import get_appsettings
import prf


class PrfTestCase(unittest.TestCase):
    def setUp(self):
        test_ini_file = os.environ.get('INI_FILE', 'test.ini')
        self.settings = get_appsettings(test_ini_file, name='main')
        self.conf = Configurator(settings=self.settings)
        prf.includeme(self.conf)
        prf.mongodb.includeme(self.conf)

    def drop_databases(self):
        c = prf.mongodb.mongo.connection.get_connection()
        c.drop_database(self.conf.registry.settings.get('mongodb.db'))
