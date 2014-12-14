from pyramid.security import ALL_PERMISSIONS, Allow


class BaseACL(object):

    __context_class__ = None

    def __init__(self, request):
        self.__acl__ = [(Allow, 'g:admin', ALL_PERMISSIONS)]
        self.__context_acl__ = [(Allow, 'g:admin', ALL_PERMISSIONS)]
        self.request = request

    @property
    def acl(self):
        return self.__acl__

    @acl.setter
    def acl(self, val):
        assert isinstance(val, tuple)
        self.__acl__.append(val)

    def context_acl(self, obj):
        return self.__context_acl__

    def __getitem__(self, key):
        assert self.__context_class__

        id_field = self.__context_class__._meta['id_field']
        obj = self.__context_class__.get(**{id_field: key})
        obj.__acl__ = self.context_acl(obj)
        obj.__parent__ = self
        obj.__name__ = key
        return obj
