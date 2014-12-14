from datetime import datetime
from urllib3 import util
from pyramid.settings import asbool

from prf.utils.utils import process_fields, split_strip


class dictset(dict):

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

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, val):
        self[key] = val

    def asbool(self, name, default=False, _set=False, pop=False):
        val = asbool(self.get(name, default))
        if _set:
            self[name] = val

        if pop:
            self.pop(name, None)

        return val

    def aslist(self, name, remove_empty=True, default=[], _set=False):
        attr = self.get(name, default) or default
        _lst = (attr if isinstance(attr, list) else attr.split(','))

        if remove_empty:
            _lst = filter(bool, _lst)

        if _set:
            self[name] = _lst
        return _lst

    def asint(self, name, default=0, _set=False):
        val = int(self.get(name, default))
        if _set:
            self[name] = val
        return val

    def asfloat(self, name, default=0.0, _set=False):
        val = float(self.get(name, default))
        if _set:
            self[name] = val
        return val

    def asdict(self, name, _type=None, _set=False):
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

        return _dict

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

    def update(self, *args, **kw):
        super(dictset, self).update(*args, **kw)
        return self

    def process_list_param(self, name, _type=None, default=None, pop=False,
                           setdefault=None):
        if _type is None:
            _type = lambda t: t

        _csv = self.get(name, '')
        if _csv and isinstance(_csv, basestring):
            self[name] = [_type(each) for each in split_strip(_csv)]

        if name not in self and setdefault is not None:
            self[name] = setdefault

        if pop:
            if default is not None:
                return self.pop(name, default)
            else:
                return self.pop(name)
        else:
            if default is not None:
                return self.get(name, default)
            else:
                return self.get(name)

    def process_bool_param(self, name, default=None):
        if name in self:
            self[name] = asbool(self[name])
        elif default is not None:
            self[name] = default

        return self.get(name, None)

    def pop_bool_param(self, name, default=False):
        if name in self:
            return asbool(self.pop(name))
        else:
            return default

    def process_datetime_param(self, name):
        if name in self:
            try:
                self[name] = datetime.strptime(self[name], '%Y-%m-%dT%H:%M:%SZ'
                        )
            except ValueError:
                raise ValueError("Bad format for '%s' param. Must be ISO 8601, YYYY-MM-DDThh:mm:ssZ"
                                  % name)

        return self.get(name, None)

    def process_float_param(self, name, default=None):
        if name in self:
            try:
                self[name] = float(self[name])
            except ValueError:
                raise ValueError('%s must be a decimal' % name)
        elif default is not None:

            self[name] = default

    def process_int_param(self, name, default=None):
        if name in self:
            try:
                self[name] = int(self[name])
            except ValueError:
                raise ValueError('%s must be a decimal' % name)
        elif default is not None:

            self[name] = default

    def process_dict_param(self, name, _type=None, pop=False):
        return self.asdict(name, _type, _set=not pop)

    def pop_by_values(self, val):
        for k, v in self.items():
            if v == val:
                self.pop(k)
        return self
