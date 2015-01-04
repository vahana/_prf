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

    AccountView.set_user_model(user_model)

    authn_policy = AuthTktAuthenticationPolicy(secret,
            callback=config.maybe_dotted(AccountView.groupfinder))

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
    config.set_root_factory(config.maybe_dotted(root_factory))

    config.registry._auth = True

    config.add_route('prf_login', '%s/%s' % (route_prefix, login_path))
    config.add_view(view=AccountView, attr='login',
                    route_name='prf_login', request_method='POST')

    config.add_route('prf_logout', '%s/%s' % (route_prefix, logout_path))
    config.add_view(view=AccountView, attr='logout',
                    route_name='prf_logout')

def includeme(config):
    config.add_directive('enable_auth', enable_auth)


class AccountView(object):

    __user_model = None

    def __init__(self, request):
        self.request = request

    @classmethod
    def set_user_model(cls, model):

        def check_callable(model, name):
            if not getattr(model, name, None):
                raise AttributeError("%s model must have '%s' class method"
                                     % (model, name))

        check_callable(model, 'groupfinder')
        check_callable(model, 'authenticate')

        cls.__user_model = model

    @classmethod
    def groupfinder(cls, userid, request):
        return cls.__user_model.groupfinder(userid, request)

    def login(self):
        login = self.request.params['login']
        password = self.request.params['password']
        next = self.request.params.get('next', '')

        success, user = self.__user_model.authenticate(login, password)
        if success:
            headers = remember(self.request, user)
            if next :
                return JHTTPFound(headers=headers, location=next)
            return JHTTPOk(headers=headers)
        else:
            raise JHTTPUnauthorized("User '%s' failed to Login" % login)

    def logout(self):
        next = self.request.params.get('next', '')

        headers = forget(self.request)

        if next:
            return JHTTPFound(headers=headers, location=next)

        return JHTTPOk(headers=headers)
