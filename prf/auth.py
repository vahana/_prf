import os
import logging
import copy
import pyramid.security as pysec
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import prf.exc
from prf.utils.convert import asbool
from prf.utils import dictset, maybe_dotted, with_metaclass


log = logging.getLogger(__name__)


class BaseACL(object):
    _admin_group_name = 'g:admin'

    def __init__(self, request):
        self.request = request
        self.__acl__ = self._acl()
        self.init()

    @property
    def view(self):
        if not self.request.matched_route:
            raise dictset.DKeyError('no matched route for request')

        rname = self.request.matched_route.name
        resource = self.request.resource_map[rname]
        return resource.view

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
            raise dictset.DValueError('`%s._model` can not be None'\
                              % self.view)
        return self.view._model.get_resource(**{self.view._id_name:key})

    def __getitem__(self, key):
        item = self.get_item(key)
        item.__acl__ = self._item_acl(item)

        item.__parent__ = self
        item.__name__ = str(key)
        return item


def includeme(config):
    settings = dictset(config.get_settings())
    auth_params = settings.get_tree('auth',
                                defaults=dict(hashalg='sha512',
                                              http_only=True,
                                              callback=None,
                                              secret=None))

    if not auth_params.callback:
        raise dictset.DValueError('Missing auth.callback')
    if not auth_params.secret:
        raise dictset.DValueError('Missing auth.secret')

    auth_params.callback = maybe_dotted(auth_params.callback)
    authn_policy = AuthTktAuthenticationPolicy(**auth_params.subset(
                    ['callback', 'hashalg', 'http_only', 'secret']))

    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())

