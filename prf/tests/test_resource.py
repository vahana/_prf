import unittest
import mock
from webtest import TestApp

from pyramid import testing
from pyramid.config import Configurator
from pyramid.url import route_path

from prf.view import BaseView


def get_test_view_class(name=''):
    class View(BaseView):

        def __init__(self, *a, **k):
            BaseView.__init__(self, *a, **k)
            # turning off before and after calls
            self._before_calls = {}
            self._after_calls = {}

        def index(self, **a):
            return name + 'index'

        def show(self, **a):
            return name + 'show'

        def delete(self, **a):
            return name + 'delete'

        def __getattr__(self, attr):
            return lambda *a, **k: name + attr

    return View


def _create_config():
    config = Configurator(autocommit=True)
    config.include('prf')
    return config


class Test(unittest.TestCase):

    def setUp(self):
        self.config = _create_config()
        self.config.begin()

    def tearDown(self):
        self.config.end()
        del self.config


class DummyCrudView(object):

    def __init__(self, request):
        self.request = request

    def index(self, **a):
        return 'index'

    def show(self, **a):
        return 'show'

    def delete(self, **a):
        return 'delete'

    def __getattr__(self, attr):
        return lambda *a: attr


class TestResourceGeneration(Test):

    def test_basic_resources(self):
        from prf.resource import add_resource
        add_resource(self.config, DummyCrudView, 'message', 'messages')

        self.assertEqual(
            '/messages',
            route_path('messages', testing.DummyRequest())
        )
        self.assertEqual(
            '/messages/new',
            route_path('new_message', testing.DummyRequest())
        )
        self.assertEqual(
            '/messages/1',
            route_path('message', testing.DummyRequest(), id=1)
        )
        self.assertEqual(
            '/messages/1/edit',
            route_path('edit_message', testing.DummyRequest(), id=1)
        )

    def test_resources_with_path_prefix(self):
        from prf.resource import add_resource

        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            path_prefix='/category/:category_id'
        )

        self.assertEqual(
            '/category/2/messages',
            route_path('messages', testing.DummyRequest(), category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/new',
            route_path('new_message', testing.DummyRequest(), category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/1',
            route_path('message', testing.DummyRequest(), id=1, category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/1/edit',
            route_path(
                'edit_message',
                testing.DummyRequest(),
                id=1, category_id=2
            )
        )

    def test_resources_with_path_prefix_with_trailing_slash(self):
        from prf.resource import add_resource
        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            path_prefix='/category/:category_id/'
        )

        self.assertEqual(
            '/category/2/messages',
            route_path('messages', testing.DummyRequest(), category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/new',
            route_path('new_message', testing.DummyRequest(), category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/1',
            route_path('message', testing.DummyRequest(), id=1, category_id=2)
        )
        self.assertEqual(
            '/category/2/messages/1/edit',
            route_path(
                'edit_message', testing.DummyRequest(), id=1, category_id=2)
        )

    def test_resources_with_name_prefix(self):
        from prf.resource import add_resource
        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            name_prefix="special_"
        )

        self.assertEqual(
            '/messages/1',
            route_path('special_message', testing.DummyRequest(), id=1)
        )


class TestResourceRecognition(Test):

    def setUp(self):
        from prf.resource import add_resource
        self.config = _create_config()
        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            renderer='string'
        )
        self.config.begin()
        self.app = TestApp(self.config.make_wsgi_app())
        self.collection_path = '/messages'
        self.collection_name = 'messages'
        self.member_path = '/messages/:id'
        self.member_name = 'message'

    def test_get_collection(self):
        self.assertEqual(self.app.get('/messages').body, 'index')

    def test_get_collection_json(self):
        from prf.resource import add_resource
        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            renderer='json'
        )
        self.assertEqual(self.app.get('/messages').body, '"index"')

    def test_get_collection_prf_json(self):
        from prf.resource import add_resource
        add_resource(
            self.config,
            DummyCrudView,
            'message',
            'messages',
            renderer='prf_json'
        )
        self.assertEqual(self.app.get('/messages').body, '"index"')

    def test_get_collection_no_renderer(self):
        from prf.resource import add_resource
        add_resource(self.config, DummyCrudView, 'message', 'messages')
        self.assertRaises(ValueError, self.app.get, '/messages')

    def test_post_collection(self):
        result = self.app.post('/messages').body
        self.assertEqual(result, 'create')

    def test_get_member(self):
        result = self.app.get('/messages/1').body
        self.assertEqual(result, 'show')

    def test_put_member(self):
        result = self.app.put('/messages/1').body
        self.assertEqual(result, 'update')

    def test_delete_member(self):
        result = self.app.delete('/messages/1').body
        self.assertEqual(result, 'delete')

    def test_new_member(self):
        result = self.app.get('/messages/new').body
        self.assertEqual(result, 'new')

    def test_edit_member(self):
        result = self.app.get('/messages/1/edit').body
        self.assertEqual(result, 'edit')


class TestResource(Test):

    def test_default_view(self, *a):
        from prf.resource import Resource, default_view

        m = Resource(
            self.config,
            member_name='group_member',
            collection_name='group_members'
        )

        self.assertEqual(
            "prf.tests.unittests.views.group_members:GroupMembersView",
            default_view(m)
        )

        # singular
        m = Resource(self.config, member_name='group_member')
        self.assertEqual(
            "prf.tests.unittests.views.group_member:GroupMemberView",
            default_view(m)
        )

    def test_singular_resource(self, *a):
        View = get_test_view_class()
        config = _create_config()
        r = config.get_root_resource()
        r.add('thing', view=View)
        gm = r.add('grandpa', 'grandpas', view=View)
        wf = gm.add('wife', view=View, renderer='string')
        wf.add('child', 'children', view=View)

        config.begin()
        app = TestApp(config.make_wsgi_app())

        self.assertEqual(
            '/grandpas/1/wife',
            route_path('grandpa_wife', testing.DummyRequest(), grandpa_id=1)
        )

        self.assertEqual(
            '/grandpas/1/wife/new',
            route_path(
                'grandpa_new_wife', testing.DummyRequest(), grandpa_id=1)
        )

        self.assertEqual(
            '/grandpas/1/wife/edit',
            route_path(
                'grandpa_edit_wife', testing.DummyRequest(), grandpa_id=1)
        )

        self.assertEqual(
            '/grandpas/1/wife/children/2',
            route_path(
                'grandpa_wife_child', testing.DummyRequest(), grandpa_id=1, id=2)
        )

        self.assertEqual(
            '/grandpas/1/wife/children/new',
            route_path(
                'grandpa_wife_new_child', testing.DummyRequest(), grandpa_id=1, id=2)
        )

        self.assertEqual(
            app.put('/grandpas/1').body,
            app.post('/grandpas/1', params=dict(_method='put')).body
        )

        self.assertEqual(
            app.delete('/grandpas/1').body,
            app.post('/grandpas/1', params=dict(_method='delete')).body
        )

        self.assertEqual(
            app.put('/thing').body,
            app.post('/thing', params=dict(_method='put')).body
        )

        self.assertEqual(
            app.delete('/thing').body,
            app.post('/thing', params=dict(_method='delete')).body
        )

        self.assertEqual(
            app.put('/grandpas/1/wife').body,
            app.post('/grandpas/1/wife', params=dict(_method='put')).body
        )

        self.assertEqual(
            app.delete('/grandpas/1/wife').body,
            app.post('/grandpas/1/wife', params=dict(_method='delete')).body
        )

        self.assertEqual('"show"', app.get('/grandpas/1').body)
        self.assertEqual("show", app.get('/grandpas/1/wife').body)
        self.assertEqual('"show"', app.get('/grandpas/1/wife/children/1').body)

    def test_renderer_override(self, *args):
        # resource.renderer and view._default_renderer are only used when
        # accept header is missing.

        View = get_test_view_class()
        config = _create_config()
        r = config.get_root_resource()

        r.add('thing', 'things', renderer='string', view=View)
        r.add('2thing', '2things', renderer='json', view=View)
        r.add('3thing', '3things', view=View)  # defaults to prf_json

        config.begin()
        app = TestApp(config.make_wsgi_app())

        # no headers, user renderer==string.returns string
        self.assertEqual("index", app.get('/things').body)

        # header is sting, renderer is string. returns string
        self.assertEqual('index', app.get('/things',
                                          headers={'ACCEPT': 'text/plain'}).body)

        # header is json, renderer is string. returns json
        self.assertEqual('"index"', app.get('/things',
                                            headers={'ACCEPT': 'application/json'}).body)

        # no header. returns json
        self.assertEqual('"index"', app.get('/2things').body)

        # header==json, renderer==json, returns json
        self.assertEqual('"index"', app.get('/2things',
                                            headers={'ACCEPT': 'application/json'}).body)

        # header==text, renderer==json, returns string
        self.assertEqual("index", app.get('/2things',
                                          headers={'ACCEPT': 'text/plain'}).body)

        # no header, no renderer. uses default_renderer, returns
        # View._default_renderer==prf_json
        self.assertEqual('"index"', app.get('/3things').body)

        self.assertEqual('"index"', app.get('/3things',
                                            headers={'ACCEPT': 'application/json'}).body)

        self.assertEqual('index', app.get('/3things',
                                          headers={'ACCEPT': 'text/plain'}).body)

        # bad accept.defaults to json
        self.assertEqual('"index"', app.get('/3things',
                                            headers={'ACCEPT': 'text/blablabla'}).body)

    def test_nonBaseView_default_renderer(self, *a):
        config = _create_config()
        r = config.get_root_resource()
        r.add('ything', 'ythings', view=get_test_view_class())

        config.begin()
        app = TestApp(config.make_wsgi_app())

        self.assertEqual('"index"', app.get('/ythings').body)

    def test_nested_resources(self, *a):
        config = _create_config()
        root = config.get_root_resource()

        aa = root.add('a', 'as', view=get_test_view_class('A'))
        bb = aa.add('b', 'bs', view=get_test_view_class('B'))
        cc = bb.add('c', 'cs', view=get_test_view_class('C'))
        dd = cc.add('d', 'ds', view=get_test_view_class('D'))

        config.begin()
        app = TestApp(config.make_wsgi_app())

        app.get('/as/1/bs/2/cs/3/ds/4')

# @mock.patch('prf.resource.add_tunneling')


class TestMockedResource(Test):

    def test_get_root_resource(self, *args):
        from prf.resource import Resource

        root = self.config.get_root_resource()
        w = root.add('whatver', 'whatevers')
        self.assertIsInstance(root, Resource)
        self.assertIsInstance(w, Resource)
        self.assertEqual(root, self.config.get_root_resource())

    def test_resource_repr(self, *args):
        r = self.config.get_root_resource()
        bl = r.add('blabla')
        assert "Resource(uid='blabla')" == str(bl)

    def test_resource_exists(self, *a):
        r = self.config.get_root_resource()
        r.add('blabla')
        self.assertRaises(ValueError, r.add, 'blabla')

    def test_get_ancestors(self, *args):
        from prf.resource import Resource
        m = Resource(self.config)

        self.assertEqual([], m.ancestors)

        gr = m.add('grandpa', 'grandpas')
        pa = m.add('parent', 'parents', parent=gr)
        ch = m.add('child', 'children', parent=pa)

        self.assertListEqual([gr, pa], ch.ancestors)

    def test_resource_uid(self, *arg):
        from prf.resource import Resource
        m = Resource(self.config)
        self.assertEqual(m.uid, '')

        a = m.add('a', 'aa')
        self.assertEqual('a', a.uid)

        c = a.add('b', 'bb').add('c', 'cc')
        self.assertEqual('a_b_c', c.uid)

    @mock.patch('prf.resource.add_resource')
    def test_add_resource(self, *arg):
        from prf.resource import Resource

        View = get_test_view_class()
        m_add_resource = arg[0]

        m_hand = arg[1]
        m_hand.return_value = View

        m = Resource(self.config)
        g = m.add('grandpa', 'grandpas')

        m_add_resource.assert_called_once_with(
            self.config,
            View,
            'grandpa',
            'grandpas',
            renderer=View._default_renderer
        )

        pr = g.add('parent', 'parents')

        m_add_resource.assert_called_with(
            self.config,
            View,
            'parent',
            'parents',
            path_prefix='grandpas/:grandpa_id',
            name_prefix='grandpa_',
            renderer=View._default_renderer
        )

        ch = pr.add('child', 'children')

        m_add_resource.assert_called_with(
            self.config,
            View,
            'child',
            'children',
            path_prefix='grandpas/:grandpa_id/parents/:parent_id',
            name_prefix='grandpa_parent_',
            renderer=View._default_renderer
        )

        self.assertEqual(ch.uid, 'grandpa_parent_child')

    @mock.patch('prf.resource.add_resource')
    def test_add_resource_with_parent_param(self, *arg):
        from prf.resource import Resource
        View = get_test_view_class()
        m_add_resource = arg[0]
        m_hand = arg[1]
        m_hand.return_value = View

        m = Resource(self.config)
        g = m.add('grandpa', 'grandpas')

        m.add('parent', 'parents', parent='grandpa')
        m_add_resource.assert_called_with(
            self.config,
            View,
            'parent',
            'parents',
            path_prefix='grandpas/:grandpa_id',
            name_prefix='grandpa_',
            renderer=View._default_renderer
        )

        gm = m.add('grandma', 'grandmas')

        pa = m.add('parent', 'parents', parent=gm)
        m_add_resource.assert_called_with(
            self.config,
            View,
            'parent',
            'parents',
            path_prefix='grandmas/:grandma_id',
            name_prefix='grandma_',
            renderer=View._default_renderer
        )

        pa.add('child', 'children', parent='grandpa_parent')
        m_add_resource.assert_called_with(
            self.config,
            View,
            'child',
            'children',
            path_prefix='grandpas/:grandpa_id/parents/:parent_id',
            name_prefix='grandpa_parent_',
            renderer=View._default_renderer
        )

    @mock.patch('prf.resource.add_resource')
    def test_add_resources_from(self, *arg):
        root = self.config.get_root_resource()
        gm = root.add('grandma', 'grandmas')
        pa = gm.add('parent', 'parents')
        boy = pa.add('boy', 'boys')
        grchild = boy.add('child', 'children')
        girl = pa.add('girl', 'girls')

        self.assertEqual(len(root.resource_map), 5)

        gp = root.add('grandpa', 'grandpas')
        gp.add_from(pa)

        self.assertEqual(
            pa.children[0],
            root.resource_map['grandma_parent_boy']
        )
        self.assertEqual(
            gp.children[0].children[1],
            root.resource_map['grandpa_parent_girl']
        )
        self.assertEqual(len(root.resource_map), 10)

        # make sure these are not same objects but copies.
        self.assertNotEqual(girl, gp.children[0].children[1])

    def test_include_exclude_actions(self, *arg):
        from prf.view import BaseView
        root = self.config.get_root_resource()

        class H1(BaseView):
            pass
        r = root.add(
            'thing1',
            'things1',
            view=H1,
            exclude=['index', 'delete']
        )
        self.assertEqual(set('show update create edit new'.split()), r.actions)

        r = root.add(
            'a_thing',
            'a_things',
            view=BaseView,
            include=['blabla', 'show']
        )
        self.assertEqual(set(['show']), r.actions)

        r = root.add(
            'b_thing',
            'b_things',
            view=BaseView,
            exclude=['index', 'show', 'delete'],
            include=['index']
        )
        self.assertEqual(set(['index']), r.actions)

    def test_exclude_action_exceptions(self, *args):
        from pyramid.config import Configurator
        from prf.view import BaseView
        from prf.resource import includeme

        class UsersView(BaseView):

            def __init__(self, context, request):
                BaseView.__init__(self, context, request)

            def show(self, id):
                return u'John Doe'

        config = Configurator(autocommit=True)
        includeme(config)

        r = config.get_root_resource()
        r.add('user', 'users', view=UsersView,
              exclude=['index', 'delete'])

        app = TestApp(config.make_wsgi_app())

        # methods implemented by view classes work
        app.get('/users/1', status=200)

        # methods used in prf and explicitly excluded return 405
        app.get('/users', status=405)
        app.delete('/users/1', status=405)

        # methods not used in prf return 404
        app.options('/users', status=404)
