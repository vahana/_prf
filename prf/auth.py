from pyramid.security import ALL_PERMISSIONS, Allow
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

from prf.utils import dictset

class RootACL(object):

    __acl__ = [(Allow, 'g:admin', ALL_PERMISSIONS)]

    def __init__(self, request):
        pass

    def __getitem__(self, key):
        return type('Dummy', (object, ), {'__acl__': RootACL.__acl__})()


def enable_auth(config, user_model=None, root_factory=RootACL,
                login_path='login', logout_path='logout',
                route_prefix=''):

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

    config.add_route('prf_login', '%s/%s' % (route_prefix, login_path))
    config.add_view(view='prf.utility_views.AccountView', attr='login',
                    route_name='prf_login', request_method='POST')

    config.add_route('prf_logout', '%s/%s' % (route_prefix, logout_path))
    config.add_view(view='prf.utility_views.AccountView', attr='logout',
                    route_name='prf_logout')

def includeme(config):
    config.add_directive('enable_auth', enable_auth)
