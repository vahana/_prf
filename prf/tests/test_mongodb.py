from prf.tests.prf_testcase import PrfTestCase
from prf.dataset import get_namespaces


class TestMongoDB(PrfTestCase):
    def test_get_namespaces(self):
        assert get_namespaces() == ['default', 'prf-test2']
