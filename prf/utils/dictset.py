from prf.utils.utils import process_fields
from prf.utils.convert import *


class dictset(dict):

    """Named dict, with some set functionalities

        dset = dictset(a=1,b={'c':1})
        dset.a == dset['a'] == 1
        dset.b.c == 1
        dset.subset(['a']) == {'a':1} == dset.subset('-b')

    """

    def __init__(self, *arg, **kw):
        super(dictset, self).__init__(*arg, **kw)
        self.to_dictset()

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, val):
        if isinstance(val, dict):
            val = dictset(val)
        self[key] = val

    def __delattr__(self, key):
        self.pop(key)

    def to_dictset(self):
        for key, val in self.items():
            if isinstance(val, dict):
                self[key] = dictset(val)
            if isinstance(val, list):
                new_list = []
                for each in val:
                    if isinstance(each, dict):
                        new_list.append(dictset(each))
                    else:
                        new_list.append(each)
                self[key] = new_list

        return self

    def copy(self):
        return dictset(super(dictset, self).copy())

    def subset(self, keys):
        only, exclude = process_fields(keys)

        if only and exclude:
            raise ValueError('Can only supply either positive or negative keys, but not both'
                             )

        if only:
            return dictset([[k, v] for (k, v) in self.items() if k in only])
        elif exclude:
            return dictset([[k, v] for (k, v) in self.items() if k
                           not in exclude])

        return dictset()

    def remove(self, keys):
        only, _ = process_fields(keys)
        return dictset([[k, v] for (k, v) in self.items() if k not in only])

    def update(self, d_):
        super(dictset, self).update(dictset(d_))
        return self

    def pop_by_values(self, val):
        for k, v in self.items():
            if v == val:
                self.pop(k)
        return self

    def mget(self, prefix, defaults={}):
        if prefix[-1] != '.':
            prefix += '.'

        _dict = dictset(defaults)
        for key, val in self.items():
            if key.startswith(prefix):
                _k = key.partition(prefix)[-1]
                if val:
                    _dict[_k] = val
        return _dict

    def asbool(self, *arg, **kw):
        return asbool(self, *arg, **kw)

    def aslist(self, *arg, **kw):
        return aslist(self, *arg, **kw)

    def asint(self, *arg, **kw):
        return asing(self, *arg, **kw)

    def asfloat(self, *arg, **kw):
        return asfloat(self, *arg, **kw)

    def asdict(self, *arg, **kw):
        return asdict(self, *arg, **kw)

    def as_datetime(self, *arg, **kw):
        return as_datetime(self, *arg, **kw)
