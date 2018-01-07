import os
import unittest
from os import environ
from pyramid.config import Configurator
from pyramid.paster import get_appsettings
from pyramid import testing
from pymongo import MongoClient

import prf

class PrfTestCase(unittest.TestCase):
    def setUp(self):
        test_ini_file = environ.get('INI_FILE', 'test.ini')
        self.settings = get_appsettings(test_ini_file, 'prf')
        request = testing.DummyRequest()
        self.conf = testing.setUp(request=request, settings=self.settings)

        prf.includeme(self.conf)

        self.db = MongoClient(
            self.settings.get('mongodb.host', 'localhost'),
            self.settings.get('mongodb.port', 27017),
        )
        self.db.drop_database(self.settings.get('mongodb.db'))



    def drop_databases(self):
        self.c.drop_database(self.settings.get('mongodb.db'))
        testing.tearDown()
