import os
import pytest
import mock
from pyramid.config import Configurator
from pyramid.paster import get_appsettings
import prf

test_ini_file = os.environ.get('INI_FILE', 'test.ini')
settings = get_appsettings(test_ini_file, name='main')


class TestPRF(object):
    def test_includeme(self):
        conf = Configurator(settings=settings)
        prf.includeme(conf)

        assert hasattr(conf, 'get_root_resource')
        assert hasattr(conf, 'add_error_view')
        assert hasattr(conf, 'add_account_views')

        assert 'prf.root_resources' in conf.registry
        assert 'prf.resources_map' in conf.registry
        assert 'prf.auth' in conf.registry

        assert conf.registry['prf.resources_map'] \
            == prf.get_resource_map(conf)

        assert conf.get_root_resource() == prf.get_root_resource(conf)

    def test_add_account_views(self):
        conf = Configurator(settings=settings)

        # prf.includeme(conf)

        prf.add_account_views(conf, mock.MagicMock())

    def test_add_error_view(self):
        conf = Configurator(settings=settings)
        prf.add_error_view(conf, KeyError)
