import mock
from prf.tests.prf_testcase import PrfTestCase
from prf.mongodb import get_document_cls, connect_dataset_aliases


class TestMongoDB(PrfTestCase):
    def setUp(self):
        super(TestMongoDB, self).setUp()
        self.drop_databases()
        self.unload_documents()

    def test_get_document_cls(self):
        cls = self.create_collection('default', 'col1')
        cls2 = self.create_collection('prf-test2', 'col2')
        cls3 = self.create_collection('default', 'col3')
        cls4 = self.create_collection('prf-test2', 'col3')

        dcls = get_document_cls('col1')
        dcls2 = get_document_cls('col2')
        dcls3 = get_document_cls('col3')
        assert cls == dcls
        assert cls2 == dcls2
        assert dcls2._meta['db_alias'] == 'prf-test2'
        # This is broken behavior with collision on collection names across dbs,
        # get_document_cls will return the most recently defined class with that name.
        assert dcls3 == cls4

    @mock.patch('prf.mongodb.mongo_connect')
    def test_connect_dataset_aliases_missing_config(self, connect):
        del self.conf.registry.settings['dataset.namespaces']
        connect_dataset_aliases(self.conf, self.conf.prf_settings())
        connect.assert_not_called()
