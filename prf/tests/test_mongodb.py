import unittest

from prf.tests.prf_testcase import PrfTestCase
from prf.mongodb import get_document_cls


class TestMongoDB(PrfTestCase):
    def setUp(self):
        super(TestMongoDB, self).setUp()
        self.drop_databases()

