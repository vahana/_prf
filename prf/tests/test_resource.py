import mock
import pytest
from pyramid.config import Configurator

from prf.resource import Resource, get_view_class, get_uri_elements
from prf.view import BaseView    

conf = Configurator()
conf.include('prf')

class TestResource(object):

    def test_init_(self):
        res = Resource(conf)
        assert res.member_name == ''
        assert res.collection_name == ''
        assert res.parent == None
        assert res.uid == ''

        with pytest.raises(ValueError):
            #member name cant be empty
            res.add('', view=BaseView)

    def test_repr_(self):
        res = Resource(conf, 'member', 'collection', uid='uid')
        assert 'uid' in res.__repr__()

    def test_get_ancestors(self):
        root = Resource(conf) 

        one = root.add('one', view=BaseView)
        assert one.get_ancestors() == []

        two = one.add('two', view=BaseView)
        anc = two.get_ancestors()
        assert anc[0] == one

    def test_add(self):
        root = Resource(conf)
        two = root.add('two', view=BaseView)

        assert two.parent == root
        assert two.member_name == 'two'
        assert two.collection_name == 'twos'
        assert two.uid == 'two'
        assert two.is_singular is False

        three = two.add('tree', 'trix', view=BaseView)
        assert three.parent == two
        assert three.member_name == 'tree'
        assert three.collection_name == 'trix'
        assert three.uid == 'two:tree'
        assert three.is_singular is False
        assert three in two.children

        sing = two.add('sing', collection_name=None, view=BaseView)
        assert sing.is_singular is True

        pref = root.add('five', prefix='pref', view=BaseView)
        assert pref.uid == 'pref:five'

    @mock.patch('prf.resource.maybe_dotted')
    def test_get_view_class(self, fake_maybe_dotted):
        root = Resource(conf)

        fake_maybe_dotted.return_value = BaseView
        assert get_view_class(BaseView, root) == BaseView

        assert get_view_class('prf.view.BaseView', root) == BaseView
        fake_maybe_dotted.reset_mock()

    def test_get_uri_elements(self):
        conf.route_prefix = 'route_prefix'
        root = Resource(conf)
        ppref, npref = get_uri_elements(
                root.add('one', view=BaseView).add('two', view=BaseView))

        assert ppref == 'ones/{one_id}'
        assert npref == 'route_prefix:one:'


