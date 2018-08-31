import unittest
from slovar import slovar

from prf.view import BaseView
from prf.request import PRFRequest
from prf.tests.prf_testcase import PrfTestCase


class TestView(PrfTestCase):
    def request(self, url='/', method='GET', mime='application/json', params={}):
        from pyramid import testing
        req = testing.DummyRequest(path=url, params=params)
        req.context = testing.DummyResource()
        req.content_type=mime
        req.params = slovar({'mixed': lambda: params})
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
        d = slovar(a=1, b=[], c={'d': 2, 'e': ''})
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
        d = slovar(a=1, b=[], c={'d': 2, 'e': ''})
        result = view._process(d, False)
        assert result['data'] == {'a': 1, 'b': [], 'c': {'d': 2, 'e': ''}}

    def test_params_prop(self):
        import pytest

        request = self.request(params={'a':1})
        view = BaseView({}, request)
        assert type(view._params) == slovar

        view._params = {'b':1}
        assert type(view._params) == slovar

        with pytest.raises(ValueError):
            view._params = 'whatever the fuck I want'
