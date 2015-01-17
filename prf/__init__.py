import logging
from pkg_resources import get_distribution


APP_NAME = __package__.split('.')[0]
_DIST = get_distribution(APP_NAME)
PROJECTDIR = _DIST.location
__version__ = _DIST.version

log = logging.getLogger(__name__)


def get_root_resource(config):
    from prf.resource import Resource
    return config.registry._root_resources.setdefault(config.package_name,
            Resource(config))


def get_resource_map(request):
    return request.registry._resources_map


MSG_MAP = {
    KeyError: 'Missing param: %s',
    ValueError: 'Bad value: %s',
    AttributeError: 'Missing value: %s',
    TypeError: 'Bad type: %s'
}

def add400views(config, exc_list):

    def view(context, request):
        from prf.json_httpexceptions import JHTTPBadRequest
        msg = MSG_MAP.get(context.__class__, '%s')
        return JHTTPBadRequest(msg % context.message, request=request)

    for exc in exc_list:
        config.add_view(view, context=exc)


def includeme(config):
    from prf.renderers import JsonRendererFactory

    log.info('%s %s' % (APP_NAME, __version__))
    config.add_directive('get_root_resource', get_root_resource)
    config.add_renderer('json', JsonRendererFactory)

    config.registry._root_resources = {}
    config.registry._resources_map = {}

    config.add_request_method(get_resource_map, 'resource_map', reify=True)

    config.add_route('options', '/*path', request_method='OPTIONS')
    config.add_view('prf.utility_views.OptionsView', route_name='options')

    config.registry._auth = False

    config.set_root_factory('prf.auth.RootACL')
    config.add_directive('add400views', add400views)
