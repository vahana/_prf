import mock
import pytest
from prf.tests.prf_testcase import PrfTestCase
from pyramid.exceptions import ConfigurationExecutionError

from prf.resource import Resource, get_view_class, get_parent_elements
from prf.view import BaseView


class TestResource(PrfTestCase):
    def test_init_(self):
        res = Resource(self.conf)
        assert res.member_name == ''
        assert res.collection_name == ''
        assert res.parent == None
        assert res.uid == ''

        with pytest.raises(ValueError):
            #member name cant be empty
            res.add('', view=BaseView)

    def test_repr_(self):
        res = Resource(self.conf, 'member', 'collection', uid='uid')
        assert 'uid' in res.__repr__()

    def test_get_ancestors(self):
        root = Resource(self.conf)
        one = root.add('one', view=BaseView)
        assert one.get_ancestors() == []

        two = one.add('two', view=BaseView)
        anc = two.get_ancestors()
        assert anc[0] == one

    def test_add(self):
        root = Resource(self.conf)
        two = root.add('two', view=BaseView, id_name='two')

        assert two.parent == root
        assert two.member_name == 'two'
        assert two.collection_name == 'twos'
        assert two.uid == 'twos'
        assert two.is_singular is False

        three = two.add('tree', 'trix', view=BaseView, id_name='three')
        assert three.parent == two
        assert three.member_name == 'tree'
        assert three.collection_name == 'trix'
        assert three.uid == 'twos:trix'
        assert three.is_singular is False
        assert three in two.children

        four = three.add('four', view=BaseView)

        sing = two.add('sing', collection_name=None, view=BaseView)
        assert sing.is_singular is True

        pref = root.add('five', prefix='pref', view=BaseView)
        assert pref.uid == 'pref:fives'

    def test_add_id_name(self):
        root = Resource(self.conf)
        two = root.add('two', view=BaseView, id_name='username')
        assert two.id_name == 'username'

        three = two.add('tree', view=BaseView, id_name='username')

        assert three.path == 'twos/{two_username}/trees'

    @mock.patch('prf.resource.maybe_dotted')
    def test_get_view_class(self, fake_maybe_dotted):
        root = Resource(self.conf)

        fake_maybe_dotted.return_value = BaseView
        assert get_view_class(BaseView, root) == BaseView

        assert get_view_class('prf.view.BaseView', root) == BaseView
        fake_maybe_dotted.reset_mock()

    def test_get_parent_elements(self):
        root = Resource(self.conf)
        ppref, npref = get_parent_elements(
                root.add('one', view=BaseView).add('two', view=BaseView).add('three', view=BaseView))

        assert ppref == 'ones/{one_id}/twos/{two_id}'
        assert npref == 'ones:twos:'

    @pytest.mark.skip('route_prefix is broken')
    def test_get_parent_elements_w_route_prefix(self):
        self.conf.route_prefix = 'route_prefix'
        root = Resource(self.conf)
        ppref, npref = get_parent_elements(
                root.add('one', view=BaseView).add('two', view=BaseView).add('three', view=BaseView))

        assert ppref == 'route_prefix/ones/{one_id}/twos/{two_id}'
        assert npref == 'route_prefix:ones:'


