import os
import logging
from pkg_resources import get_distribution

from prf.utils import maybe_dotted, dictset, DKeyError, DValueError

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


def add_error_view(config, exc, http_exc=None, error='', error_attr='message'):
    import prf.exc
    exc = maybe_dotted(exc)
    http_exc = maybe_dotted(http_exc or prf.exc.HTTPBadRequest)

    def view(context, request):
        err = getattr(context, error_attr, 'empty error message')

        msg = error % str(err) if error else err
        return http_exc(msg, request=request)

    log.info('add_error_view: %s -> %s' % (exc.__name__, http_exc.__name__))
    config.add_view(view, context=exc)


def add_account_views(config, user_model, route_prefix=''):
    from pyramid.security import  NO_PERMISSION_REQUIRED
    from prf.utility_views import AccountView

    user_model = config.maybe_dotted(user_model)
    AccountView.set_user_model(user_model)

    route_tmpl = '%s%%s' % (route_prefix + ':' if route_prefix else '')

    for action, method in [['login', 'POST'], ['logout', 'POST']]:
        route_name = route_tmpl % action
        config.add_route(route_name,
                         '%s' % os.path.join(route_prefix, action))

        config.add_view(view=AccountView, attr=action, route_name=route_name,
                        request_method=method,
                        renderer='json',
                        permission=NO_PERMISSION_REQUIRED)


def add_api_view(config):
    from pyramid.security import  NO_PERMISSION_REQUIRED
    from prf.utility_views import APIView

    config.add_route('prf_api','/')
    config.add_view(view=APIView, attr='show', route_name='prf_api',
                    request_method='GET',
                    renderer='json',
                    permission=NO_PERMISSION_REQUIRED)


def set_default_acl(config, acl_model):
    acl_model = maybe_dotted(acl_model)
    config.set_root_factory(acl_model)


def process_tweens(config):
    import pyramid
    for tween in dictset(config.registry.settings).aslist('tweens', sep='\n', default=''):
        config.add_tween(tween)


def disable_exc_tweens(config, names=None):
    names = names or ['pyramid.tweens.excview_tween_factory',
                      'prf.mongodb.mongodb_exc_tween']

    from pyramid.interfaces import ITweens
    tweens = config.registry.queryUtility(ITweens)
    for name in names:
        log.warning('`%s` tween disabled' % name)
        tweens.sorter.remove(name)


def prf_settings(config):
    import sys
    from pyramid.scripts.common import parse_vars

    # When running unit tests, sys.argv is pytest's options
    # Added a `testing` setting to prevent trying to load the settings file
    if not config.registry.settings.get('testing'):
        try:
            config_file = sys.argv[1]
        except IndexError:
            raise ValueError('No config file provided')

        return dictset(config.registry.settings).update_with(parse_vars(sys.argv[2:]))
    return dictset(config.registry.settings)


def includeme(config):
    log.info('%s %s' % (APP_NAME, __version__))
    settings = prf_settings(config)

    config.add_directive('get_root_resource', get_root_resource)
    config.add_directive('add_error_view', add_error_view)
    config.add_directive('set_default_acl', set_default_acl)
    config.add_directive('disable_exc_tweens', disable_exc_tweens)
    config.add_directive('prf_settings', prf_settings)

    config.add_renderer('json', maybe_dotted('prf.renderers.JsonRenderer'))

    config.registry['prf.root_resources'] = {}
    config.registry['prf.resources_map'] = {}
    config.registry['prf.auth'] = settings.asbool('auth.enabled', default=False)
    config.registry['prf.xhr'] = settings.asbool('xhr.enabled', default=False)

    config.add_request_method(get_resource_map, 'resource_map', reify=True)

    config.add_route('options', '/*path', request_method='OPTIONS')
    config.add_view('prf.utility_views.OptionsView', route_name='options')

    process_tweens(config)

    add_error_view(config, DKeyError, error='Missing param: %s')
    add_error_view(config, DValueError, error='Bad value: %s')

    from pyramid import httpexceptions
    import prf.exc

    # replace html versions of pyramid http exceptions with json versions
    add_error_view(config, httpexceptions.HTTPUnauthorized, prf.exc.HTTPUnauthorized)
    add_error_view(config, httpexceptions.HTTPForbidden, prf.exc.HTTPForbidden)
    add_error_view(config, httpexceptions.HTTPNotFound, prf.exc.HTTPNotFound)

    config.add_directive('add_account_views', add_account_views)
    config.add_directive('add_api_view', add_api_view)

    if settings.asbool('show_api', default=True):
        config.add_api_view()

    config.set_root_factory(RootFactory)


def main(*args, **kwargs):
    pass
