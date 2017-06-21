from slovar import slovar as basedictset
from prf.utils.convert import *
from prf.utils.utils import json_dumps


class dictset(basedictset):
    def asbool(self, *arg, **kw):
        return asbool(self, *arg, **kw)

    def aslist(self, *arg, **kw):
        return aslist(self, *arg, **kw)

    def asset(self, *arg, **kw):
        return asset(self, *arg, **kw)

    def asset(self, *arg, **kw):
        return self.aslist(*arg, unique=True, **kw)

    def asint(self, *arg, **kw):
        return asint(self, *arg, **kw)

    def asfloat(self, *arg, **kw):
        return asfloat(self, *arg, **kw)

    def asdict(self, *arg, **kw):
        return asdict(self, *arg, **kw)

    def asdt(self, *arg, **kw):
        return asdt(self, *arg, **kw)

    def asstr(self, *arg, **kw):
        return asstr(self, *arg, **kw)

    def asrange(self, *arg, **kw):
        return asrange(self, *arg, **kw)

    def asqs(self, *arg, **kw):
        return asqs(self, *arg, **kw)

    def json(self):
        return json_dumps(self)

    def __setattr__(self, key, val):
        if isinstance(val, dict):
            val = dictset(val)
        self[key] = val

    def copy(self):
        return dictset(super(dictset, self).copy())

    def extract(self, *args, **kwargs):
        return dictset(super(dictset, self).extract(*args, **kwargs))

    def get_by_prefix(self, *args, **kwargs):
        return dictset(super(dictset, self).get_by_prefix(*args, **kwargs))

    def subset(self, *args, **kwargs):
        return dictset(super(dictset, self).subset(*args, **kwargs))

    def get_tree(self, *args, **kwargs):
        return dictset(super(dictset, self).get_tree(*args, **kwargs))

    def transform(self, *args, **kwargs):
        return dictset(super(dictset, self).transform(*args, **kwargs))

    def flat(self, *args, **kwargs):
        return dictset(super(dictset, self).flat(*args, **kwargs))

    def unflat(self, *args, **kwargs):
        return dictset(super(dictset, self).unflat(*args, **kwargs))

    def update_with(self, *args, **kwargs):
        return dictset(super(dictset, self).update_with(*args, **kwargs))

    def pop_many(self, *args, **kwargs):
        return dictset(super(dictset, self).pop_many(*args, **kwargs))
