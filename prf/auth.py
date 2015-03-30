import os
import logging
from pyramid.security import ALL_PERMISSIONS, Allow
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import prf.exc
from prf.utils.convert import asbool
from prf.utils import dictset, maybe_dotted
from prf.utils.utils import DKeyError, DValueError

log = logging.getLogger(__name__)

class BaseACL(object):
    __acl__ = []    
    def __init__(self, request):
        self.request = request
        self.init()

    def init(self):
        pass


def includeme(config):
    settings = dictset(config.get_settings())
    auth_params = settings.mget('auth',
                                defaults=dict(hashalg='sha512',
                                              http_only=True,
                                              callback=None,
                                              secret=None))

    if not auth_params.callback:
        raise DValueError('Missing auth.callback')
    if not auth_params.secret:
        raise DValueError('Missing auth.secret')

    auth_params.callback = maybe_dotted(auth_params.callback)
    authn_policy = AuthTktAuthenticationPolicy(**auth_params.subset(
                    ['callback', 'hashalg', 'http_only', 'secret']))

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())

