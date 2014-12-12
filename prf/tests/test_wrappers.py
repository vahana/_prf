#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import mock
from prf import wrappers
from prf.view import BaseView


class WrappersTest(unittest.TestCase):

    def test_update_links(self):
        res = wrappers.update_links({}, [])
        self.assertEqual({}, res)

        res = wrappers.update_links({}, {})
        self.assertEqual({}, res)

        res = wrappers.update_links('trash', 'garbage')
        self.assertEqual('trash', res)

        with self.assertRaises(ValueError):
            res = wrappers.update_links({'trash': 111}, 'garbage')

        with self.assertRaises(TypeError):
            # must fail, since items not dicts
            res = wrappers.update_links({}, [1, 2])

        res = wrappers.update_links({}, {'self': 'url'})
        self.assertEqual({'link': {'self': 'url'}}, res)

        res = wrappers.update_links({}, {'self': 'url1', 'other': 'url2'})
        self.assertEqual({'links': {'self': 'url1', 'other': 'url2'}}, res)

        res = wrappers.update_links({'link': {}}, {'self': 'url'})
        self.assertEqual({'link': {'self': 'url'}}, res)

        res = wrappers.update_links({'link': {'a': 'b'}}, {'self': 'url'})
        self.assertEqual({'links': {'a': 'b', 'self': 'url'}}, res)

        res = wrappers.update_links({'links': {}}, {'self': 'url'})
        self.assertEqual({'link': {'self': 'url'}}, res)

        res = wrappers.update_links({'links': {'a': 'b', 'c': 'd'}},
                                    {'self': 'url'})
        self.assertEqual({'links': {'a': 'b', 'c': 'd', 'self': 'url'}}, res)

    def test_validator_decorator(self):
        params = dict(a=10, b='bbb', c=20)

        req = mock.MagicMock(params=params)
        res = mock.MagicMock(actions=['create', 'update', 'index'])

        class MyView(BaseView):

            __validation_schema__ = dict(a=dict(type=int, required=True),
                                         b=dict(type=str, required=False))

            def __init__(self):
                BaseView.__init__(self, res, req)

            @wrappers.validator(c=dict(type=int, required=True),
                                a=dict(type=float, required=False))
            def create(self):
                pass

            @wrappers.validator()
            def update(self):
                pass

            @wrappers.validator(a=dict(type=int, required=False))
            def index(self):
                []

        view = MyView()
        self.assertEqual([wrappers.validate_types(),
                          wrappers.validate_required()],
                         view._before_calls['create'])
        self.assertIn('c', view._before_calls['create'][0].kwargs)

        self.assertEqual(dict(type=float, required=False),
                         view._before_calls['create'][0].kwargs['a'])

    def test_add_parent_links(self):
        request = mock.MagicMock()

        res = wrappers.add_parent_links(request=request, result={})
        self.assertEqual({}, res)

        def route_url(name, id):
            if name == 'thing':
                return 'http://localhost/thing/%s' % id
            else:
                raise KeyError

        request.route_url = route_url
        res = wrappers.add_parent_links(request=request, result=dict(one_id=1,
                                                                     one='blabla', thing_id=3))

        self.assertEqual(res['thing'], {'link': 'http://localhost/thing/3',
                                        'id': 3})
        self.assertEqual(res['one_id'], 1)
        self.assertEqual(res['one'], 'blabla')

        res = wrappers.add_parent_links(request=request,
                                        result=dict(thing_id=None))
        self.assertEqual({'thing': {'link': 'http://localhost/thing/None',
                                    'id': None}}, res)

        # multiple results
        res = wrappers.add_parent_links(request=request,
                                        result=[dict(one_id=1, one='blabla', thing_id=3),
                                                dict(one_id=2, one='blabla', thing_id=6)])

        self.assertEqual([{'one': 'blabla', 'one_id': 1, 'thing': {'id': 3,
                                                                   'link': 'http://localhost/thing/3'}}, {'one': 'blabla', 'one_id': 2, 'thing': {'id': 6,
                                                                                                                                                  'link': 'http://localhost/thing/6'}}], res)

    def test_validate_types(self):
        import datetime as dt

        request = mock.MagicMock()
        wrappers.validate_types()(request=request)

        schema = dict(a=dict(type=int), b=dict(type=str),
                      c=dict(type=dt.datetime), d=dict(type=dt.date),
                      e=dict(type=None), f=dict(type='BadType'))

        request.params = dict(a=1, b=2)
        wrappers.validate_types(**schema)(request=request)

        request.params = dict(c='2000-01-01T01:01:01')
        wrappers.validate_types(**schema)(request=request)

        request.params = dict(d='2000-01-01')
        wrappers.validate_types(**schema)(request=request)

        request.params = dict(c='bad_date')
        with self.assertRaises(wrappers.ValidationError):
            wrappers.validate_types(**schema)(request=request)

        request.params = dict(d='bad_date')
        with self.assertRaises(wrappers.ValidationError):
            wrappers.validate_types(**schema)(request=request)

        request.params = dict(e='unknown_type')
        with mock.patch('prf.wrappers.log') as log:
            wrappers.validate_types(**schema)(request=request)
            self.assertTrue(log.debug.called)

        request.params = dict(f='bad_type')
        with self.assertRaises(wrappers.ValidationError):
            wrappers.validate_types(**schema)(request=request)

    def test_validate_required(self):
        request = mock.MagicMock()
        wrappers.validate_types()(request=request)

        schema = dict(a=dict(type=int, required=True), b=dict(type=str,
                                                              required=False), c=dict(type=int))

        request.params = dict(a=1, b=2, c=3)
        wrappers.validate_required(**schema)(request=request)

        request.params = dict(a=1, b=2)
        wrappers.validate_required(**schema)(request=request)

        request.params = dict(a=1, c=3)
        wrappers.validate_required(**schema)(request=request)

        request.params = dict(b=2, c=3)
        with self.assertRaises(wrappers.ValidationError):
            wrappers.validate_required(**schema)(request=request)

    def test_obj2dict(self):
        result = mock.MagicMock()
        result.to_dict.return_value = dict(a=1)

        res = wrappers.obj2dict(result=result)
        self.assertEqual(dict(a=1), res)

        result.to_dict.return_value = [dict(a=1), dict(b=2)]
        self.assertEqual([dict(a=1), dict(b=2)],
                         wrappers.obj2dict(result=result))

        special = mock.MagicMock()
        special.to_dict.return_value = {'special': 'dict'}
        result = ['a', 'b', special]
        self.assertEqual(['a', 'b', {'special': 'dict'}],
                         wrappers.obj2dict(result=result))

        self.assertEqual([], wrappers.obj2dict(result=[]))
