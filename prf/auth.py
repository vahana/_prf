import os
import logging
import copy
import pyramid.security as pysec
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import prf.exc
from prf.utils.convert import asbool
from prf.utils import dictset, maybe_dotted, with_metaclass
from prf.utils.utils import DKeyError, DValueError
from prf.resource import Action


log = logging.getLogger(__name__)


class BaseACL(object):
    _model = None
    _id_name = 'id'
    _admin_group_name = 'g:admin'

    def __init__(self, request):
        self.request = request

        self.__acl__ = self._acl()
        self.init()

    def init(self):
        pass

    def _acl(self):
        return [(pysec.Allow, self._admin_group_name, pysec.ALL_PERMISSIONS)] +\
                self.acl()

    def acl(self):
        return []

    def _item_acl(self, item):
        return [(pysec.Allow, self._admin_group_name, pysec.ALL_PERMISSIONS)] +\
                self.item_acl(item)

    def item_acl(self, item):
        return []

    def get_item(self, key):
        if not self._model:
            raise DValueError('`%s._model` can not be None'\
                              % self.__class__.__name__)
        return self._model.get_resource(**{self._id_name:key})

    def __getitem__(self, key):
        item = self.get_item(key)
        item.__acl__ = self._item_acl(item)

        item.__parent__ = self
        item.__name__ = key
        return item


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

