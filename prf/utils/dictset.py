from prf.utils.utils import process_fields, DKeyError, DValueError
from prf.utils.convert import *


def get_rec(d, path):
    for seg in path:
        d = d[seg]
    if not isinstance(d, dict):
        raise DValueError('`%s` must be (derived from) dict' % d)

    return d

def extend(d1, d2, prefix_keys=None):
    """
    from prf.utils import dictset, extend
    d1 = dictset({'a':{'b':{'c':1}}})
    d2 = dictset({'a':{'b':{'d':1}}})
    extend(d1, d2)

    """
    if prefix_keys is None:
        prefix_keys = []

    d1_ = get_rec(d1, prefix_keys)
    d2_ = get_rec(d2, prefix_keys)

    for key, val in d2_.items():
        if key not in d1_:
            d1_.update(d2_)
            return

        prefix_keys.append(key)
        extend(d1, d2, prefix_keys)


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
        if key.startswith('__'): # dont touch the special attributes
            return super(dictset, self).__getattr__(key)

        try:
            return self[key]
        except KeyError as e:
            raise DKeyError(e.message)

    def __missing__(self, key):
        raise DKeyError(key)

    def __setattr__(self, key, val):
        if isinstance(val, dict):
            val = dictset(val)
        self[key] = val

    def __delattr__(self, key):
        self.pop(key)

    def __contains__(self, item):
        if isinstance(item, (tuple, list, set)):
            return bool(set(self.keys()) & set(item))
        else:
            return super(dictset, self).__contains__(item)

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
            raise DValueError('Can only supply either positive or negative keys, but not both'
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

    def extend(self, d_):
        extend(self, d_)
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

    @classmethod
    def from_dotted(cls, dotkey, val):
        # 'a.b.c', 100 -> {a:{b:{c:100}}}

        key, _, sufix = dotkey.partition('.')
        if not sufix:
            return cls({key:val})
        return cls({key: cls.from_dotted(sufix, val)})

    def has(self, key, check_type):
        if key in self:
            if not isinstance(self[key], check_type):
                raise DValueError('`%s` must be `%s`' % (key, check_type))
            return True
        return False

    def asbool(self, *arg, **kw):
        return asbool(self, *arg, **kw)

    def aslist(self, *arg, **kw):
        return aslist(self, *arg, **kw)

    def asint(self, *arg, **kw):
        return asint(self, *arg, **kw)

    def asfloat(self, *arg, **kw):
        return asfloat(self, *arg, **kw)

    def asdict(self, *arg, **kw):
        return asdict(self, *arg, **kw)

    def as_datetime(self, *arg, **kw):
        return as_datetime(self, *arg, **kw)
