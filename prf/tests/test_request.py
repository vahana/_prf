import mock
from prf.tests.prf_testcase import PrfTestCase
from prf.request import Request

class TestRequest(PrfTestCase):
    def setUp(self):
        super(TestRequest, self).setUp()

    @mock.patch('__builtin__.open')
    @mock.patch('prf.request.Request.get')
    def test_download(self, mocked_get, mocked_file):
        api = Request('')

        resp = api.download('some_bogus_url/file_name.xyz')
        mocked_get.assert_called()
        mocked_file.assert_called()
