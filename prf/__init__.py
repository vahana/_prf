import logging
from pkg_resources import get_distribution
from pyramid import httpexceptions


import prf.exc
from prf.utils import maybe_dotted, aslist
from prf.utils.utils import DKeyError, DValueError

APP_NAME = __package__.split('.')[0]
_DIST = get_distribution(APP_NAME)
PROJECTDIR = _DIST.location
__version__ = _DIST.version

log = logging.getLogger(__name__)


def get_root_resource(config):
    from prf.resource import Resource
    return config.registry._root_resources.setdefault(config.package_name,
            Resource(config, uid=config.route_prefix))


def get_resource_map(request):
    return request.registry._resources_map


def add_error_view(config, exc, http_exc=None, cond='', error=''):
    http_exc = maybe_dotted(http_exc or prf.exc.HTTPBadRequest)

    def view(context, request):
        if cond in (context.message or context.detail or context.explanation or ''):
            msg = error % context.message if error else context.message
            return http_exc(msg, request=request)

    log.info('add_error_view: %s -> %s' % (exc.__name__, http_exc.__name__))
    config.add_view(view, context=exc)


def process_tweens(config):
    import pyramid
    for tween in aslist(config.registry.settings, 'tweens', sep='\n', default=''):
        config.add_tween(tween, over=pyramid.tweens.MAIN)


def includeme(config):
    log.info('%s %s' % (APP_NAME, __version__))
    config.add_directive('get_root_resource', get_root_resource)
    config.add_directive('add_error_view', add_error_view)

    config.add_renderer('json', maybe_dotted('prf.renderers.JsonRenderer'))

    config.registry._root_resources = {}
    config.registry._resources_map = {}

    config.add_request_method(get_resource_map, 'resource_map', reify=True)

    config.add_route('options', '/*path', request_method='OPTIONS')
    config.add_view('prf.utility_views.OptionsView', route_name='options')

    config.registry._auth = False

    config.set_root_factory('prf.auth.RootACL')

    process_tweens(config)

    add_error_view(config, DKeyError, error='Missing param: %s')
    add_error_view(config, DValueError, error='Bad value: %s')

    # replace html versions of pyramid http exceptions with json versions
    add_error_view(config, httpexceptions.HTTPUnauthorized, prf.exc.HTTPUnauthorized)
    add_error_view(config, httpexceptions.HTTPForbidden, prf.exc.HTTPForbidden)
    add_error_view(config, httpexceptions.HTTPNotFound, prf.exc.HTTPNotFound)
