import os
import logging
from pyramid.security import ALL_PERMISSIONS, Allow
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.security import remember, forget

import prf.exc
from prf.utils import dictset

log = logging.getLogger(__name__)


class RootACL(object):

    __acl__ = [(Allow, 'g:admin', ALL_PERMISSIONS)]

    def __init__(self, request):
        pass

    def __getitem__(self, key):
        return type('DummyContext', (object, ), {'__acl__': RootACL.__acl__,
                    '__repr__': lambda self: \
                    '%s: ACL for this resource is not provided.' \
                    % self.__class__})()


def enable_auth(config, user_model=None, root_factory=RootACL,
                login_path='login', logout_path='logout', route_prefix=''):


    settings = dictset(config.registry.settings)
    auth_params = settings.mget('auth', defaults=dict(hashalg='sha512',
                                http_only=True,
                                callback=config.maybe_dotted(AccountView.groupfinder)))

    if 'secret' not in auth_params:
        # try old place before cursing
        try:
            auth_params['secret'] = settings.auth_tkt_secret
            log.warning('Deprecated: auth_tkt_secret is deprecated, use auth.secret instead')
        except KeyError:
            raise KeyError('auth.secret is missing')

    user_model = config.maybe_dotted(user_model)

    AccountView.set_user_model(user_model)

    authn_policy = AuthTktAuthenticationPolicy(**auth_params)

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
    config.set_root_factory(config.maybe_dotted(root_factory))

    config.registry._auth = True

    route_name = '%s_login' % route_prefix
    config.add_route(route_name,
                     '%s' % os.path.join(route_prefix, login_path))
    config.add_view(view=AccountView, attr='login', route_name=route_name,
                    request_method='POST')

    route_name = '%s_logout' % route_prefix
    config.add_route(route_name,
                     '%s' % os.path.join(route_prefix, logout_path))
    config.add_view(view=AccountView, attr='logout', route_name=route_name)


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
            if next:
                return prf.exc.HTTPFound(headers=headers, location=next)
            return prf.exc.HTTPOk(headers=headers)
        else:
            raise prf.exc.HTTPUnauthorized("User '%s' failed to Login" % login)

    def logout(self):
        next = self.request.params.get('next', '')

        headers = forget(self.request)

        if next:
            return prf.exc.HTTPFound(headers=headers, location=next)

        return prf.exc.HTTPOk(headers=headers)
