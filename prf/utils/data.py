from prf.utils.dictset import dictset


class DataProxy(object):

    def __init__(self, data={}):
        self._data = dictset(data)

    def to_dict(self, keys=None, nested=False, depth=10):
        keys = keys or []
        data = (self._data.subset(keys) if keys else self._data)
        _dict = dictset()

        for attr, val in data.items():
            _dict[attr] = val
            if depth:
                kw = dict(nested=True, depth=depth - 1)
                if hasattr(val, 'to_dict'):
                    _dict[attr] = val.to_dict(**kw)
                elif isinstance(val, list):
                    _dict[attr] = to_dicts(val, **kw)

        return _dict


def dict2obj(data):
    if not data:
        return data

    top = type(str(data.get('_type')), (DataProxy, ), {})(data)

    for key, val in top._data.items():
        key = key.encode('ascii', 'ignore')
        if isinstance(val, dict):
            setattr(top, key, dict2obj(val))
        elif isinstance(val, list):
            setattr(top, key, [(dict2obj(sj) if isinstance(sj, dict) else sj)
                    for sj in val])
        else:
            setattr(top, key, val)

    return top


def to_objs(collection):
    _objs = []

    for each in collection:
        _objs.append(dict2obj(each))

    return _objs


def to_dicts(collection, key=None, **kw):
    _dicts = []
    try:
        for each in collection:
            try:
                each_dict = each.to_dict(**kw)
                if key:
                    each_dict = key(each_dict)
                _dicts.append(each_dict)
            except AttributeError, e:
                _dicts.append(each)
    except TypeError:
        return collection

    return _dicts


def obj2dict(obj, classkey=None):
    if isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = obj2dict(obj[k], classkey)
        return obj
    elif hasattr(obj, '__iter__'):
        return [obj2dict(v, classkey) for v in obj]
    elif hasattr(obj, '__dict__'):
        data = dictset([(key, obj2dict(value, classkey)) for (key, value) in
                       obj.__dict__.iteritems() if not callable(value)
                       and not key.startswith('_')])
        if classkey is not None and hasattr(obj, '__class__'):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj
