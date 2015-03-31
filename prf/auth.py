import os
import logging
import copy
from pyramid.security import ALL_PERMISSIONS, Allow, Everyone
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import prf.exc
from prf.utils.convert import asbool
from prf.utils import dictset, maybe_dotted, with_metaclass
from prf.utils.utils import DKeyError, DValueError
from prf.resource import Action

log = logging.getLogger(__name__)


class MetaACL(type):
    def __new__(mcs, name, bases, attrs):
        klass = super(MetaACL, mcs).__new__(mcs, name, bases, attrs)
        klass.__acl__ = klass._collection_acl()
        return klass        


class BaseACL(with_metaclass(MetaACL, object)):
    _model = None
    _id_name = 'id'

    def __init__(self, request):
        self.request = request
        self.init()

    def init(self):
        pass

    @classmethod
    def _collection_acl(cls):
        return [(Allow, 'g:admin', ALL_PERMISSIONS)] + cls.collection_acl()

    @classmethod
    def collection_acl(cls):
        return []

    def _resource_acl(self, resource):
        return [(Allow, 'g:admin', ALL_PERMISSIONS)] + self.resource_acl(resource)

    def resource_acl(self, resource):
        return []

    def get_resource(self, key):
        if not self._model:
            raise DValueError('`%s._model` can not be None'\
                              % self.__class__.__name__)            
        return self._model.get_resource(**{self._id_name:key})

    def __getitem__(self, key):
        obj = self.get_resource(key)
        obj.__acl__ = self._resource_acl(obj)

        obj.__parent__ = self
        obj.__name__ = key
        return obj        


class OpenACL(BaseACL):
    @classmethod
    def collection_acl(cls):
        return [
            (Allow, Everyone, [Action.SHOW, Action.INDEX])
        ]

    def resource_acl(self):
        return [
            (Allow, Everyone, [Action.SHOW, Action.INDEX])
        ]


class SecureACL(BaseACL):
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

