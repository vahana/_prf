import os
import logging
from pkg_resources import get_distribution
from pyramid import httpexceptions
from pyramid.security import  NO_PERMISSION_REQUIRED

import prf.exc
from prf.utils import maybe_dotted, aslist, dictset
from prf.utils.utils import DKeyError, DValueError
from prf.utility_views import AccountView

APP_NAME = __package__.split('.')[0]
_DIST = get_distribution(APP_NAME)
PROJECTDIR = _DIST.location
__version__ = _DIST.version

log = logging.getLogger(__name__)


class RootFactory(object):
    def __init__(self, request):
        pass #pragma nocoverage

    def __getitem__(self, key):
        return type('DummyContext', (object, ), {})()


def get_root_resource(config):
    from prf.resource import Resource
    return config.registry['prf.root_resources'].setdefault(config.package_name,
            Resource(config, uid=config.route_prefix))


def get_resource_map(request):
    return request.registry['prf.resources_map']


def add_error_view(config, exc, http_exc=None, cond='', error=''):
    exc = maybe_dotted(exc)
    http_exc = maybe_dotted(http_exc or prf.exc.HTTPBadRequest)

    def view(context, request):
        if cond in (context.message or context.detail or context.explanation or ''):
            msg = error % context.message if error else context.message
            return http_exc(msg, request=request)

    log.info('add_error_view: %s -> %s' % (exc.__name__, http_exc.__name__))
    config.add_view(view, context=exc)


def add_login_views(config, user_model, route_prefix=''):

    user_model = config.maybe_dotted(user_model)
    AccountView.set_user_model(user_model)

    route_tmpl = '%s%%s' % (route_prefix + ':' if route_prefix else '')

    route_name = route_tmpl % 'login'
    config.add_route(route_name,
                     '%s' % os.path.join(route_prefix, 'login'))

    config.add_view(view=AccountView, attr='login', route_name=route_name,
                    # request_method='POST',
                    renderer='json',
                    permission=NO_PERMISSION_REQUIRED)

    route_name = route_tmpl % 'logout'
    config.add_route(route_name,
                     '%s' % os.path.join(route_prefix, 'logout'))

    config.add_view(view=AccountView, attr='logout', route_name=route_name,
                    renderer='json',
                    permission=NO_PERMISSION_REQUIRED)


def set_default_acl(config, acl_model):
    acl_model = maybe_dotted(acl_model)
    config.set_root_factory(acl_model)


def process_tweens(config):
    import pyramid
    for tween in aslist(config.registry.settings, 'tweens', sep='\n', default=''):
        config.add_tween(tween, over=pyramid.tweens.MAIN)


def includeme(config):
    log.info('%s %s' % (APP_NAME, __version__))
    settings = dictset(config.get_settings())

    config.add_directive('get_root_resource', get_root_resource)
    config.add_directive('add_error_view', add_error_view)
    config.add_directive('set_default_acl', set_default_acl)

    config.add_renderer('json', maybe_dotted('prf.renderers.JsonRenderer'))

    config.registry['prf.root_resources'] = {}
    config.registry['prf.resources_map'] = {}
    config.registry['prf.auth'] = settings.asbool('auth.enabled', False)

    config.add_request_method(get_resource_map, 'resource_map', reify=True)

    config.add_route('options', '/*path', request_method='OPTIONS')
    config.add_view('prf.utility_views.OptionsView', route_name='options')

    process_tweens(config)

    add_error_view(config, DKeyError, error='Missing param: %s')
    add_error_view(config, DValueError, error='Bad value: %s')

    # replace html versions of pyramid http exceptions with json versions
    add_error_view(config, httpexceptions.HTTPUnauthorized, prf.exc.HTTPUnauthorized)
    add_error_view(config, httpexceptions.HTTPForbidden, prf.exc.HTTPForbidden)
    add_error_view(config, httpexceptions.HTTPNotFound, prf.exc.HTTPNotFound)

    config.add_directive('add_login_views', add_login_views)

    config.set_root_factory(RootFactory)
