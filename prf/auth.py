import os
import logging
from pyramid.security import ALL_PERMISSIONS, Allow
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import prf.exc
from prf.utils.convert import asbool
from prf.utils import dictset, maybe_dotted
from prf.utility_views import AccountView

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


def includeme(config):
    settings = dictset(config.get_settings())
    auth_params = settings.mget('auth',
                                defaults=dict(hashalg='sha512',
                                              http_only=True,
                                              callback=None))

    auth_params.callback = maybe_dotted(auth_params.callback)
    authn_policy = AuthTktAuthenticationPolicy(**auth_params.subset(
                    ['callback', 'hashalg', 'http_only', 'secret']))

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
    config.set_root_factory(RootACL)

