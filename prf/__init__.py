import logging
from pkg_resources import get_distribution

from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.security import ALL_PERMISSIONS, Allow


APP_NAME = __package__.split('.')[0]
_DIST = get_distribution(APP_NAME)
PROJECTDIR = _DIST.location
__version__ = _DIST.version

log = logging.getLogger(__name__)


class RootACL(object):

    __acl__ = [(Allow, 'g:admin', ALL_PERMISSIONS)]

    def __init__(self, request):
        pass

    def __getitem__(self, key):
        return type('Dummy', (object, ), {'__acl__': RootACL.__acl__})()


def get_root_resource(config):
    from prf.resource import Resource
    return config.registry._root_resources.setdefault(config.package_name,
            Resource(config))


def get_resource_map(request):
    return request.registry._resources_map


def enable_auth(config, user_model=None, root_factory=RootACL,
                login_path='/login', logout_path='/logout'):

    secret = config.registry.settings['auth_tkt_secret']

    user_model = config.maybe_dotted(user_model)

    from prf.utility_views import AccountView
    AccountView.set_user_model(user_model)

    authn_policy = AuthTktAuthenticationPolicy(secret,
            callback=config.maybe_dotted(AccountView.groupfinder))

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
    config.set_root_factory(config.maybe_dotted(root_factory))

    config.registry._auth = True

    config.add_route('prf_login', login_path)
    config.add_view(view='prf.utility_views.AccountView', attr='login',
                    route_name='prf_login', request_method='POST')

    config.add_route('prf_logout', logout_path)
    config.add_view(view='prf.utility_views.AccountView', attr='logout',
                    route_name='prf_logout', request_method='POST')


def includeme(config):
    from prf.renderers import JsonRendererFactory

    log.info('%s %s' % (APP_NAME, __version__))
    config.add_directive('get_root_resource', get_root_resource)
    config.add_renderer('json', JsonRendererFactory)

    config.registry._root_resources = {}
    config.registry._resources_map = {}

    config.add_request_method(get_resource_map, 'resource_map', reify=True)

    config.add_tween('prf.tweens.GET_tunneling')
    config.add_tween('prf.tweens.cache_control')

    config.add_route('options', '/*path', request_method='OPTIONS')
    config.add_view('prf.utility_views.OptionsView', route_name='options')

    config.set_root_factory(RootACL)

    config.registry._auth = False

    config.add_directive('enable_auth', enable_auth)
