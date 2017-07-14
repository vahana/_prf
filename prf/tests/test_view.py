from prf.view import BaseView
from prf.request import PRFRequest
from prf.utils import dictset
from prf.tests.prf_testcase import PrfTestCase


class TestView(PrfTestCase):
    def request(self, url='/', method='GET', mime='application/json', params={}):
        req = PRFRequest(url)
        req.content_type = mime
        # Hackery so that request.params.mixed() returns things
        req.params = dictset({'mixed': lambda: params})
        req.accept = [mime]
        req.method = method
        return req

    def test_pop_empty_dict(self):
        request = self.request(params={'_pop_empty': 1})
        view = BaseView({}, request)
        result = view._process([{'a': 1, 'b': [], 'c': {'d': 2, 'e': ''}}], True)
        assert result['data'] == [{'a': 1, 'c': {'d': 2}}]

    def test_pop_empty_model(self):
        request = self.request(params={'_pop_empty': 1})
        view = BaseView({}, request)
        c = self.create_collection('default', 'col1')
        d = c(a=1, b=[], c={'d': 2, 'e': ''})
        result = view._process(d, False)
        assert result['data'] == {'a': 1, 'c': {'d': 2}}

    def test_no_pop_empty_dict(self):
        request = self.request()
        view = BaseView({}, request)
        result = view._process([{'a': 1, 'b': [], 'c': {'d': 2, 'e': ''}}], True)
        assert result['data'] == [{'a': 1, 'b': [], 'c': {'d': 2, 'e': ''}}]

    def test_no_pop_empty_model(self):
        request = self.request()
        view = BaseView({}, request)
        c = self.create_collection('default', 'col1')
        d = c(a=1, b=[], c={'d': 2, 'e': ''})
        result = view._process(d, False)
        assert result['data'] == {'a': 1, 'b': [], 'c': {'d': 2, 'e': ''}}
