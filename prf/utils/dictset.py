from datetime import datetime
from pyramid.settings import asbool

from prf.utils.utils import process_fields, split_strip


def parametrize(func):
    def wrapper(obj, name, default=None, raise_on_empty=False, pop=False,
                **kw):

        if default is None:
            try:
                value = obj[name]
            except KeyError:
                raise KeyError("Missing '%s'" % name)
        else:
            value = obj.get(name, default)

        if raise_on_empty and not value:
            raise ValueError("'%s' can not be empty" % name)

        result = func(obj, value, **kw)

        if pop:
            obj.pop(name, None)
        else:
            obj[name] = result

        return result

    return wrapper


class dictset(dict):
    def __init__(self, *arg, **kw):
        super(dictset, self).__init__(*arg, **kw)
        self.to_dictset()

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

        if only and not exclude:
            return dictset([[k, v] for (k, v) in self.items() if k in only])

        if exclude:
            return dictset([[k, v] for (k, v) in self.items() if k
                           not in exclude])

        return dictset()

    def remove(self, keys):
        only, _ = process_fields(keys)
        return dictset([[k, v] for (k, v) in self.items() if k not in only])

    def update(self, d_):
        super(dictset, self).update(dictset(d_))
        return self

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, val):
        self[key] = val

    @parametrize
    def asbool(self, value):
        return asbool(value)

    @parametrize
    def aslist(self, value, remove_empty=True):
        _lst = (value if isinstance(value, list) else value.split(','))
        return filter(bool, _lst) if remove_empty else _lst

    @parametrize
    def asint(self, value):
        return int(value)

    @parametrize
    def asfloat(self, value):
        return float(value)

    def asdict(self, name, _type=None, _set=False, pop=False):
        """
        Turn this 'a:2,b:blabla,c:True,a:'d' to {a:[2, 'd'], b:'blabla', c:True}

        """

        if _type is None:
            _type = lambda t: t

        dict_str = self.pop(name, None)
        if not dict_str:
            return {}

        _dict = {}
        for item in split_strip(dict_str):
            key, _, val = item.partition(':')
            if key in _dict:
                if type(_dict[key]) is list:
                    _dict[key].append(val)
                else:
                    _dict[key] = [_dict[key], val]
            else:
                _dict[key] = _type(val)

        if _set:
            self[name] = _dict
        elif pop:
            self.pop(name, None)

        return _dict

    def as_datetime(self, name):
        if name in self:
            try:
                self[name] = datetime.strptime(self[name], '%Y-%m-%dT%H:%M:%SZ'
                        )
            except ValueError:
                raise ValueError("Bad format for '%s' param. Must be ISO 8601, YYYY-MM-DDThh:mm:ssZ"
                                  % name)

        return self.get(name, None)

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

    def pop_by_values(self, val):
        for k, v in self.items():
            if v == val:
                self.pop(k)
        return self
