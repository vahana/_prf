from datetime import datetime
from pyramid.settings import asbool as p_asbool


def parametrize(func):

    def wrapper(dset, name, default=None, raise_on_empty=False, pop=False,
                **kw):

        if default is None:
            try:
                value = dset[name]
            except KeyError:
                raise KeyError("Missing '%s'" % name)
        else:
            value = dset.get(name, default)

        if raise_on_empty and not value:
            raise ValueError("'%s' can not be empty" % name)

        result = func(dset, value, **kw)

        if pop:
            dset.pop(name, None)
        else:
            dset[name] = result

        return result

    return wrapper


@parametrize
def asbool(dset, key):
    return p_asbool(key)


@parametrize
def aslist(dset, key, remove_empty=True):
    _lst = (key if isinstance(key, list) else key.split(','))
    return (filter(bool, _lst) if remove_empty else _lst)


@parametrize
def asint(dset, key):
    return int(key)


@parametrize
def asfloat(dset, key):
    return float(key)


def asdict(dset, name, _type=None, _set=False, pop=False):
    """
    Turn this 'a:2,b:blabla,c:True,a:'d' to {a:[2, 'd'], b:'blabla', c:True}

    """

    if _type is None:
        _type = lambda t: t

    dict_str = dset.pop(name, None)
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
        dset[name] = _dict
    elif pop:
        dset.pop(name, None)

    return _dict


def as_datetime(dset, name):
    if name in dset:
        try:
            dset[name] = datetime.strptime(dset[name], '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            raise ValueError("Bad format for '%s' param. Must be ISO 8601, YYYY-MM-DDThh:mm:ssZ"
                              % name)

    return dset.get(name, None)
