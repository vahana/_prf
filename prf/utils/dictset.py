import pprint
import urllib, re
from itertools import groupby

from prf.utils.utils import DKeyError, DValueError, split_strip, json_dumps, str2dt
from prf.utils.convert import *


def merge(d1, d2, path=None):
    if path is None: path = []

    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge(d1[key], d2[key], path + [str(key)])
        else:
            d1[key] = d2[key]
    return d1


def expand_list(param):
    _new = []
    if isinstance(param, (list, set)):
        for each in param:
            if isinstance(each, basestring) and each.find(',') != -1:
                _new.extend(split_strip(each))
            elif isinstance(each, (list, set)):
                _new.extend(each)
            else:
                _new.append(each)
    elif isinstance(param, basestring) and param.find(',') != -1:

        _new = split_strip(param)

    return _new


def process_fields(fields, parse=True):
    fields_only = []
    fields_exclude = []
    nested = {}
    show_as = {}
    show_as_r = {}
    transforms = {}
    star = False

    if isinstance(fields, basestring):
        fields = split_strip(fields)

    for field in expand_list(fields):
        field = field.strip()
        negative = False

        if not field:
            continue

        if '*' == field:
            star = True
            continue

        field,_,trans = field.partition(':')
        trans = trans.split('|') if trans else []

        if field[0] == '-':
            field = field[1:]
            negative = True

        if parse and '__as__' in field:
            root,_,val = field.partition('__as__')
            show_as[root] = val or root.split('.')[-1]
            show_as_r[val or root.split('.')[-1]]=root

            field = root

        if trans:
            if field in show_as:
                transforms[show_as[field]] = trans
            else:
                transforms[field] = trans

        if parse and '.' in field:
            root = field.split('.')[0]
            nested[field] = root
            field = root

        if negative:
            fields_exclude.append(field)
        else:
            fields_only.append(field)

    return dictset({
             'only': fields_only,
             'exclude':fields_exclude,
             'nested': nested,
             'show_as': show_as,
             'show_as_r': show_as_r,
             'transforms': transforms,
             'star': star})


class dictset(dict):

    """Named dict, with some set functionalities

        dset = dictset(a=1,b={'c':1})
        dset.a == dset['a'] == 1
        dset.b.c == 1
        dset.subset(['a']) == {'a':1} == dset.subset('-b')

    """

    DKeyError = DKeyError
    DValueError = DValueError


    @classmethod
    def to_dicts(cls, iterable, fields):
        return [e.extract(fields) for e in iterable]

    def __init__(self, *arg, **kw):
        try:
            super(dictset, self).__init__(*arg, **kw)
        except ValueError as e:
            raise DValueError(e.message)

        self.to_dictset()

    def __getattr__(self, key):
        if key.startswith('__'): # dont touch the special attributes
            raise AttributeError('Attribute error %s' % key)

        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e.message)

    def __setattr__(self, key, val):
        if isinstance(val, dict):
            val = dictset(val)
        self[key] = val

    def __delattr__(self, key):
        self.pop(key, None)

    def __contains__(self, item):
        if isinstance(item, (tuple, list, set)):
            return bool(set(self.keys()) & set(item))
        else:
            return super(dictset, self).__contains__(item)

    def __add__(self, item):
        return self.copy().update(item)

    def __iadd__(self, item):
        return self.update(item)

    def __getitem__(self, key):
        try:
            return super(dictset, self).__getitem__(key)
        except KeyError as e:
            raise DKeyError(e.message)

    def to_dict(self, fields):
        return self.extract(fields)

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

    def extract(self, fields):
        if not fields:
            return self

        only, exclude, nested, show_as, show_as_r, trans, star =\
                process_fields(fields).mget(
                               ['only','exclude', 'nested',
                                'show_as', 'show_as_r', 'transforms',
                                'star'])

        nested_keys = nested.keys()

        def process_lists(flat_d):
            for nkey, nval in nested.items():
                if '..' in nkey:
                    pref, suf = nkey.split('..', 1)
                    _lst = []

                    for ix in range(len(_d.get(nval, []))):
                        kk = '%s.%s.%s'%(pref,ix,suf)
                        _lst.append(flat_d.subset(kk))
                        ix+=1

                    new_key = '%s.%s'%(pref,suf)
                    nested_keys.append(new_key)
                    flat_d[new_key] = _lst

                    if nkey in show_as:
                        show_as[new_key] = show_as.pop(nkey)

        if star:
            _d = self
        else:
            _d = self.subset(only + ['-'+e for e in exclude])

        if nested:
            flat_d = self.flat(keep_lists=0)
            process_lists(flat_d)
            flat_d = flat_d.subset(nested_keys)
            _d.remove(nested.values())
            _d.update(flat_d)

        for new_key, key in show_as_r.items():
            if key in _d:
                _d.merge(dictset({new_key:_d.get(key)}))

        #remove old keys
        for _k in show_as_r.values():
            _d.pop(_k, None)

        for key, trs in trans.items():
            if key in _d:
                for tr in trs:
                    if tr == 'str':
                        _d[key] = unicode(_d[key])
                        continue
                    elif tr == 'unicode':
                        _d[key] = unicode(_d[key])
                        continue
                    elif tr == 'int':
                        _d[key] = int(_d[key]) if _d[key] else _d[key]
                        continue
                    elif tr == 'float':
                        _d[key] = float(_d[key]) if _d[key] else _d[key]
                        continue
                    elif tr == 'flat' and isinstance(_d[key], dictset):
                        _d[key] = _d[key].flat()
                        continue
                    elif tr == 'dt':
                        _d[key] = str2dt(_d[key])
                        continue

                    _type = type(_d[key])
                    try:
                        method = getattr(_type, tr)
                        if not callable(method):
                            raise dictset.DValueError(
                                '`%s` is not a callable for type `%s`'
                                    % (tr, _type))
                        _d[key] = method(_d[key])
                    except AttributeError as e:
                        raise dictset.DValueError(
                                'type `%s` does not have a method `%s`'
                                    % (_type, tr))

            else:
                for tr in trs:
                    if tr.startswith('='):
                        _d[key] = tr.partition('=')[2]
                        continue

        return _d.unflat()

    def get_by_prefix(self, prefix):
        if not isinstance(prefix, list):
            prefixes = [prefix]
        else:
            prefixes = prefix

        _d = dictset()
        for k,v in self.items():
            for pref in prefixes:
                _pref = pref[:-1]

                if pref.endswith('*'):
                    if k.startswith(_pref):
                        ix = _pref.rfind('.')
                        if ix > 0:
                            _pref = _pref[:ix]
                            k = k[len(_pref)+1:]
                        _d[k]=v
                else:
                    if k == pref:
                        ix = _pref.rfind('.')
                        if ix > 0:
                            _pref = _pref[:ix]
                            k = k[len(_pref)+1:]

                        _d[k]=v

        return _d.unflat()

    def subset(self, keys):

        if keys is None:
            return self

        only, exclude = process_fields(
                keys, parse=False).mget(['only','exclude'])

        _d = dictset()

        if only and exclude:
            raise DValueError(
                    'Can only supply either positive or negative keys,'
                    ' but not both')

        if only:
            prefixed = [it for it in only if it.endswith('*')]
            exact = [it for it in only if not it.endswith('*')]

            if exact:
                _d = dictset([[k, v] for (k, v)
                        in self.items() if k in exact])

            if prefixed:
                _d = _d.update_with(self.get_by_prefix(prefixed))

        elif exclude:
            _d = dictset([[k, v] for (k, v) in self.items()
                                            if k not in exclude])

        return _d

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

    def remove(self, keys):
        for k in keys:
            self.pop(k, None)
        return self

    def update(self, d_):
        super(dictset, self).update(dictset(d_))
        return self

    def merge(self, d_):
        merge(self, d_)
        return self

    def pop_by_values(self, vals):
        if not isinstance(vals, list):
            vals = [vals]

        for k, v in self.items():
            if v in vals:
                self.pop(k)
        return self

    def get_tree(self, prefix, defaults={}, sep='.'):
        if prefix[-1] != '.':
            prefix += sep

        _dict = dictset(defaults)
        for key, val in self.items():
            if key.startswith(prefix):
                _k = key.partition(prefix)[-1]
                _dict[_k] = val
        return _dict

    def mget(self, keys):
        return [self[e] for e in split_strip(keys) if e in self]

    @classmethod
    def from_dotted(cls, dotkey, val):
        # 'a.b.c', 100 -> {a:{b:{c:100}}}
        # 'a.b.1', 100 -> {a:{b:[None,100]}}

        key, _, sufix = dotkey.partition('.')

        if not sufix:
            if key.isdigit():
                _lst = [None]*int(key) + [val]
                return _lst
            else:
                return cls({key:val})

        if key.isdigit():
            _lst = [None]*int(key) + [cls.from_dotted(sufix, val)]
            return _lst
        else:
            return cls({key: cls.from_dotted(sufix, val)})

    def has(self, keys, check_type=basestring,
                        err='', _all=True, allow_missing=False,
                        allowed_values=[]):
        errors = []

        if isinstance(keys, basestring):
            keys = [keys]

        self_flat = self.flat().update(self) # update with self to include high level keys too

        def missing_key_error(_type, key):
            if _type == dict:
                missing = ['%s.%s' % (key, val) for val in allowed_values]
            else:
                missing = allowed_values

            return 'Missing key or invalid values for `%s`. Allowed values are: `%s`'\
                                          % (key, missing)

        def error_msg(msg):
            if "%s" in err:
                error = err % msg
            elif err:
                error = err
            else:
                error = msg

            errors.append(error)
            return error

        for key in keys:
            if key in self_flat:
                if check_type and not isinstance(self_flat[key], check_type):
                    error_msg(u'`%s` must be type `%s`, got `%s` instead'\
                                          % (key, check_type.__name__,
                                             type(self_flat[key]).__name__))

                if allowed_values and self_flat[key] not in allowed_values:
                    error_msg(missing_key_error(check_type, key))

            elif not allow_missing:
                if allowed_values:
                    error_msg(missing_key_error(check_type, key))
                else:
                    error_msg('Missing key: `%s`' % key)

        if (errors and _all) or (not _all and len(errors) >= len(keys)):
            raise DValueError('.'.join(errors))

        return True

    def transform(self, rules):
        _d = dictset()

        flat_dict = self.flat()
        flat_rules = rules.flat()
        # flat_dict.update(self)

        for path, val in flat_dict.items():
            if path in rules:
                _d.merge(dictset.from_dotted(rules[path], val))

        return _d

    @classmethod
    def build_from(cls, source, rules, allow_empty=True,
                    allow_missing=False, inverse=False):
        _d = dictset()

        flat_rules = dictset(rules).flat()
        flat_source = dictset(source).flat()
        flat_source.update(source)

        for key, val in flat_rules.items():
            if not val: # if val in the rule is missing, use the key
                val = key

            if inverse:
                key,val = val,key # flip em

            if key.endswith('.'):
                _val = flat_source.get_treet(key)
            else:
                if allow_missing:
                    _val = flat_source.get(key, key)
                else:
                    _val = flat_source[key]

            if _val != "" or allow_empty:
                _d[val] = _val

        return _d.unflat()

    def flat(self, keep_lists=True):
        return dictset(flat(self, keep_lists=keep_lists))

    def unflat(self):
        return dictset(unflat(self))

    def set_default(self, name, val):
        if name not in self.flat():
            self.merge(dictset.from_dotted(name, val))
        return val

    def get_first(self, keys):
        for key in keys:
            if key in self:
                return self[key]

        raise DKeyError('Neither of `%s` keys found' % keys)

    def fget(self, key, *arg, **kw):
        return self.flat().get(key, *arg, **kw)

    def deep_update(self, _dict):
        return self.flat().update(_dict.flat()).unflat()

    def update_with(self, _dict, overwrite=True, append_to=None, append_to_set=None,
                    reverse=False, exclude=[]):

        if isinstance(append_to, basestring):
            append_to = [append_to]

        if isinstance(append_to_set, basestring):
            append_to_set = [append_to_set]

        append_to = append_to or []

        _append_to_set = {}
        for each in (append_to_set or []):
            k,_,sk = each.partition(':')
            _append_to_set[k]=sk
        append_to_set = _append_to_set

        if not reverse:
            self_dict = self.copy()
        else:
            self_dict = _dict.copy()
            _dict = self

        def _append_to(key, val):
            if isinstance(self_dict[key], list):
                if isinstance(val, list):
                    self_dict[key].extend(val)
                else:
                    self_dict[key] += val
            else:
                raise DValueError('`%s` is not a list' % key)

        def _append_to_set(key, val):
            _append_to(key, val)
            set_key = append_to_set.get(key)

            if set_key:
                _uniques = []
                _met = []
                for each in self_dict[key]:
                    # if there is not set_key, it must be treated as unique
                    if set_key not in each:
                        _uniques.append(each)
                        continue

                    if each[set_key] in _met:
                        continue

                    _met.append(each[set_key])
                    _uniques.append(each)
                self_dict[key] = _uniques
            else:
                self_dict[key] = list(set(self_dict[key]))

        for key, val in _dict.items():
            if key in exclude:
                continue
            if overwrite or key not in self_dict:
                if key in append_to and key in self_dict:
                    _append_to(key, val)
                elif key in append_to_set and key in self_dict:
                    _append_to_set(key, val)
                else:
                    self_dict[key] = val

        return self_dict

    def merge_with(self, _dict, reverse=False):
        return self.update_with(_dict, overwrite=False,
                                reverse=reverse)

    def contains(self, other, exclude=None):
        other_ = other.subset(exclude)
        return not other_ or self.subset(other_.keys()) == other_

    def pop_many(self, keys):
        poped = dictset()
        for key in keys:
            poped[key] = self.pop(key, None)
        return poped

    def sensor(self, patterns):
        self_f = self.flat()
        for key in self_f:
            for each in patterns:
                if key.endswith(each):
                    self_f[key] = '******'

        return self_f.unflat()

    def json(self):
        return json_dumps(self)

    def _pp(self):
        pprint.pprint(self)


#based on jsonurl

def type_cast(value):
    return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def dot_split(s):
    return [part for part in re.split(r"(?<!\.)\.(?!\.)", s)]


def _extend_list(_list, length):
    if len(_list) < length:
        for _ in range(length - len(_list)):
            _list.append({})


def unflat(d):
    r = {}

    for k, leaf_value in d.items():
        path = k.split('.')
        prev_ctx = r
        ctx = r
        # Last item is a leaf, we save time by doing it outside the loop
        for i, part in enumerate(path[:-1]):
            # If context is a list, part should be an int
            # Testing part.isdigit() is significantly faster than isinstance(ctx, list)
            ctx_is_list = part.isdigit()
            if ctx_is_list:
                part = int(part)
            # If the next part is an int, we need to contain a list
            ctx_contains_list = path[i+1].isdigit()

            # Set the current node to placeholder value, {} or []
            if not ctx_is_list and not ctx.get(part):
                ctx[part] = [] if ctx_contains_list else {}

            # If we're dealing with a list, make sure it's big enough
            # for part to be in range
            if ctx_is_list:
                _extend_list(ctx, part + 1)

            # If we're empty and contain a list
            if not ctx[part] and ctx_contains_list:
                ctx[part] = []

            prev_ctx = ctx
            ctx = ctx[part]

        k = path[-1]
        if k.isdigit():
            k = int(k)
            _extend_list(ctx, k + 1)

        ctx[k] = leaf_value

    return r


def flat(d, key='', keep_lists=False):
    r = {}
    # Make a dict regardless, ints as keys for a list
    iterable = d if isinstance(d, dict) else dict(enumerate(d))
    for k, v in iterable.items():
        # Join keys but prevent keys from starting by '.'
        kk = k if not key else '.'.join([key, str(k)])
        # Recursion if we find a dict or list, except if we're keeping lists
        if isinstance(v, dict) or (isinstance(v, list) and not keep_lists):
            r.update(flat(v, key=kk, keep_lists=keep_lists))
        # Otherwise just set attribute
        else:
            r[kk] = v
    return r
