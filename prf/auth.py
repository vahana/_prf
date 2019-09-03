import os
import logging
import copy

import pyramid.security as pysec
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

from slovar import slovar
import prf.exc
from prf.utils import maybe_dotted, DKeyError, DValueError


log = logging.getLogger(__name__)


class BaseACL(object):
    _admin_group_name = 'g:admin'

    def __init__(self, request):
        self.request = request
        self.__acl__ = self._acl()
        self.init()

    @property
    def resource(self):
        if not self.request.matched_route:
            raise DKeyError('no matched route for request')

        rname = self.request.matched_route.name
        return self.request.resource_map[rname]

    @property
    def view(self):
        return self.resource.view

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
        return self.acl()

    def get_item(self, key):
        if not self.view._model:
            raise DValueError('`%s._model` can not be None'\
                              % self.view)
        return self.view._model.get_resource(**{self.resource.id_name:key})

    def __getitem__(self, key):
        item = self.get_item(key)
        if not item:
            return

        item.__acl__ = self._item_acl(item)
        item.__parent__ = self
        item.__name__ = str(item.id)
        return item

    @classmethod
    def role_fields(cls):
        return {}


def includeme(config):
    settings = slovar(config.get_settings())
    auth_params = settings.extract(['auth.hashlag',
                                   'auth.http_only:bool',
                                   'auth.callback',
                                   'auth.policy_class',
                                   'auth.secret',
                                   'auth.timeout:int',
                                   'auth.reissue_time:int',
                                   'auth.max_age:int'],
                                    defaults={'auth.hashalg':'sha512',
                                              'auth.http_only':True,
                                              'auth.callback':None,
                                              'auth.secret':None}).unflat().auth

    if not auth_params.callback:
        raise DValueError('Missing auth.callback')
    if not auth_params.secret:
        raise DValueError('Missing auth.secret')


    auth_policy_class = maybe_dotted(auth_params.pop('policy_class', AuthTktAuthenticationPolicy))
    auth_params.callback = maybe_dotted(auth_params.callback)
    authn_policy = auth_policy_class(**auth_params)

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
