from slovar import slovar as basedictset
from prf.utils.convert import *
from prf.utils.utils import json_dumps
from prf.utils.errors import DKeyError, DValueError

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


class dkdict(dictset):
    def raise_getattr_exc(self):
        raise DKeyError(error)

    def raise_value_exc(self, error):
        raise DValueError(error)

