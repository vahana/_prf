#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import mock
from webtest import TestApp
from pyramid.config import Configurator

from prf.view import BaseView
from prf.json_httpexceptions import *
from prf.wrappers import get_pagination_links, wrap_me
from prf import paginate
from prf.resource import includeme
from prf.tests.unittests import get_test_view_class


class TestBaseView(unittest.TestCase):

    def test_BaseView(self, *a):

        class UsersView(BaseView):

            def __init__(self, context, request):
                BaseView.__init__(self, context, request)
                self.resource.actions = ['show', 'delete']

            def show(self, id):
                return u'John Doe'

        request = mock.MagicMock(content_type='')
        request.matched_route.pattern = '/users'
        view = UsersView(request.context, request)

        self.assertEqual(u'John Doe', view.show(1))

        self.assertRaises(JHTTPMethodNotAllowed, view.index)

        with self.assertRaises(AttributeError):
            view.frobnicate()

        # delete is an allowed action, but it raises since BaseView does not implement it.
        with self.assertRaises(AttributeError):
            view.delete()


    def test_get_pagination_links(self):
        request = mock.MagicMock(content_type='')
        request.params = dict()
        request.path_url = '/things'

        ctrl = BaseView(mock.MagicMock(), request)
        ctrl.request.page = mock.MagicMock()

        ctrl.request.page.first_page = 1
        ctrl.request.page.previous_page = 1
        ctrl.request.page.next_page = 2
        ctrl.request.page.last_page = 3

        links = get_pagination_links(ctrl.request)

        self.assertEqual({
            'first': '/things?page=1',
            'last': '/things?page=3',
            'next': '/things?page=2',
            'previous': '/things?page=1',
        }, links)

        ctrl.request.page.first_page = 1
        ctrl.request.page.previous_page = None
        ctrl.request.page.next_page = None
        ctrl.request.page.last_page = 1

        links = get_pagination_links(ctrl.request)

        self.assertEqual({'first': '/things?page=1', 'last': '/things?page=1'
                         }, links)

    def test_add_links(self, *a):
        config = Configurator(autocommit=True)
        includeme(config)

        def get_results():
            return [{'id': 0, 'name': 'Bruce Lee'}, {'id': 1,
                    'name': 'Jacky Chan'}]

        class MyView(BaseView):

            def index(self):
                return get_results()

            def show(self, id):
                return get_results()[int(id)]

            def delete(self, id):
                return [1, 2, 3]

        r = config.get_root_resource()
        r.add('thing', 'things', include=['index', 'show', 'delete'],
              view=MyView)

        app = TestApp(config.make_wsgi_app())
        resp = app.get('/things/0').json

        # show must have self and edit links
        self.assertEqual({u'edit': u'http://localhost/things/0',
                         u'self': u'http://localhost/things/0'}, resp['links'])

        # index doesnt have self and edit links only pagination
        resp = app.get('/things').json
        self.assertEqual(resp.get('links'),
                          {'last': 'http://localhost/things?page=1',
                          'first': 'http://localhost/things?page=1'})

    def test_pagination(self, *a):
        num_items = 100
        self.maxDiff = None

        class MyView(BaseView):
            @paginate(items_per_page=5)
            def index(self):
                return range(num_items)

        config = Configurator(autocommit=True)
        includeme(config)

        r = config.get_root_resource()
        r.add('thing', 'things', include=['update', 'index'], view=MyView)

        app = TestApp(config.make_wsgi_app())

        resp = app.get('/things').json
        self.assertEqual({u'links': {u'first': u'http://localhost/things?page=1'
                         , u'last': u'http://localhost/things?page=20',
                         u'next': u'http://localhost/things?page=2'},
                         u'things': [0, 1, 2, 3, 4], u'total': 100}, resp)

        resp = app.get('/things?page=2').json
        self.assertEqual({u'links': {
            u'first': u'http://localhost/things?page=1',
            u'last': u'http://localhost/things?page=20',
            u'next': u'http://localhost/things?page=3',
            u'previous': u'http://localhost/things?page=1',
        }, u'things': [5, 6, 7, 8, 9], u'total': 100}, resp)

        resp = app.get('/things?page=3').json
        self.assertEqual('http://localhost/things?page=1', resp['links'
                         ]['first'])
        self.assertEqual('http://localhost/things?page=2', resp['links'
                         ]['previous'])
        self.assertEqual('http://localhost/things?page=4', resp['links']['next'
                         ])

        num_items = 3  # less than per page
        resp = app.get('/things?page=5').json

        self.assertEqual(u'http://localhost/things?page=1', resp['links'
                         ]['first'], resp['links']['last'])

        self.assertNotIn('next', resp['links'])
        self.assertNotIn('previous', resp['links'])

        self.assertEqual(num_items, int(resp['total']))

    def test_ViewMapper(self):
        from prf.view import ViewMapper

        bc1 = mock.Mock()
        bc3 = mock.Mock()
        bc2 = mock.Mock()
        ac1 = mock.Mock(return_value=['thing'])

        class MyView:

            def __init__(self, ctx, req):
                self._before_calls = {'index': [bc1], 'show': [bc3]}
                self._after_calls = {}

            @wrap_me(before=bc2)
            def index(self):
                return ['thing']

        request = mock.MagicMock()
        resource = mock.MagicMock(actions=['index'])

        wrapper = ViewMapper(**{'attr': 'index'})(MyView)
        resp = wrapper(resource, request)

        bc1.assert_called_with(request=request)

        self.assertFalse(bc2.called)
        self.assertFalse(bc3.called)

    def test_defalt_wrappers_and_wrap_me(self):
        from prf import wrappers

        self.maxDiff = None

        def before_call(*a):
            return a[2]

        def after_call(*a):
            return a[2]

        class MyView(BaseView):

            @wrappers.wrap_me(before=before_call, after=after_call)
            def index(self):
                return [1, 2, 3]

        request = mock.MagicMock(content_type='')
        resource = mock.MagicMock(actions=['index'])
        view = MyView(resource, request)

        self.assertEqual(view._after_calls, {'index': [
            wrappers.pager(),
            wrappers.obj2dict,
            wrappers.add_parent_links,
            wrappers.wrap_in_dict,
            wrappers.add_pagination_links,
            after_call,
        ], 'show': [wrappers.obj2dict, wrappers.add_self_links,
                    wrappers.add_parent_links]})

        self.assertEqual({'create': [wrappers.validate_types(),
                         wrappers.validate_required()],
                         'index': [before_call],
                         'update': [wrappers.validate_types()]},
                         view._before_calls)

        self.assertEqual(view.index._before_calls, [before_call])
        self.assertEqual(view.index._after_calls, [after_call])

