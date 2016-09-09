import urllib, re
from collections import OrderedDict

from prf.utils.utils import DKeyError, DValueError, split_strip, json_dumps
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

    def __init__(self, *arg, **kw):
        try:
            super(dictset, self).__init__(*arg, **kw)
        except ValueError as e:
            raise DValueError(e.message)

        self.to_dictset()

    def __getattr__(self, key):
        if key.startswith('__'): # dont touch the special attributes
            return super(dictset, self).__getattr__(key) #pragma nocoverage

        try:
            return self[key]
        except KeyError as e:
            raise DKeyError(e.message)

    def __setattr__(self, key, val):
        if key == '_ordered_dict':
            return super(dictset, self).__setattr__(key, val)

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

        only, exclude, nested, show_as, trans, star =\
                process_fields(fields).mget(
                               ['only','exclude', 'nested',
                                'show_as', 'transforms',
                                'star'])

        nested_keys = nested.keys()

        def process_lists(flat_d):
            for nkey, nval in nested.items():
                if '..' in nkey:
                    pref, suf = nkey.split('..', 1)
                    _lst = []

                    for ix in range(len(_d.get(nval, []))):
                        kk = '%s.%s.%s'%(pref,ix,suf)
                        _lst.append(flat_d.pop(kk, None))
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
            flat_d = _d.flat()
            process_lists(flat_d)

            flat_d = flat_d.subset(nested_keys)
            _d.remove(nested.values())
            _d.update(flat_d)

        for key, new_key in show_as.items():
            if key in _d:
                _d.merge(dictset({new_key:_d.pop(key)}))

        for key, trs in trans.items():
            if key in _d:
                for tr in trs:
                    if tr == 'str':
                        _d[key] = str(_d[key])
                        continue
                    elif tr == 'unicode':
                        _d[key] = unicode(_d[key])
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
            _d = dictset([[k, v] for (k, v)
                                in self.items() if k in only])
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

    def pop(self, *arg, **kw):
        if hasattr(self, '_ordered_dict'):
            self._ordered_dict.pop(*arg, **kw)
        return super(dictset, self).pop(*arg, **kw)

    def flat(self, keep_lists=True):
        _flat = dict_to_args(self, keep_lists=keep_lists)
        _d = dictset(_flat)
        _d._ordered_dict = _flat
        return _d

    def unflat(self):
        if hasattr(self, '_ordered_dict'):
            _d = dictset(args_to_dict(self._ordered_dict))
            del self._ordered_dict
            return _d

        return dictset(args_to_dict(self))

    def set_default(self, name, val):
        if name not in self.flat():
            self.merge(dictset.from_dotted(name, val))
        return val

    def get_first(self, keys):
        for key in keys:
            if key in self:
                return self[key]

        raise DKeyError('Neither of `%s` keys found' % keys)

    def fget(self, key, default=None):
        return self.flat().get(key, default)

    def deep_update(self, _dict):
        return self.flat().update(_dict.flat()).unflat()

    def update_with(self, _dict, overwrite=True, append_to=None,
                    reverse=False, exclude=[]):
        append_to = append_to or []
        if not reverse:
            self_dict = self.copy()
        else:
            self_dict = _dict.copy()
            _dict = self

        for key, val in _dict.items():
            if key in exclude:
                continue
            if overwrite or key not in self_dict:
                if append_to and key in append_to:
                    if isinstance(self_dict[key], list):
                        if isinstance(val, list):
                            self_dict[key].extend(val)
                        else:
                            self_dict[key] += val
                    else:
                        raise DValueError('`%s` is not a list' % key)
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
        [self.pop(key, None) for key in keys]
        return self

    def sensor(self, patterns):
        self_f = self.flat()
        for key in self_f:
            for each in patterns:
                if key.endswith(each):
                    self_f[key] = '******'

        return self_f.unflat()

    def json(self):
        return json_dumps(self)

#based on jsonurl

def type_cast(value):
    return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def list_to_args(l):
    args = OrderedDict()
    pos = 0
    for i in l:
        if isinstance(i, dict):
            sub = dict_to_args(i)
            for s, nv in sub.items():
                args[str(pos) + "." + s] = nv
        elif isinstance(i, list):
            sub = list_to_args(i)
            for s, nv in sub.items():
                args[str(pos) + "." + s] = nv
        else:
            args[str(pos)] = i
        pos += 1
    return args


def dict_to_args(d, keep_lists=False):
    args = OrderedDict()
    for k, v in d.items():
        if isinstance(v, dict):
            sub = dict_to_args(v, keep_lists=keep_lists)
            for s, nv in sub.items():
                args["%s.%s" % (k,s)] = nv
        elif isinstance(v, list) and not keep_lists:
            sub = list_to_args(v)
            for s, nv in sub.items():
                args["%s.%s" % (k,s)] = nv
        else:
            args[k] = v
    return args


def dot_split(s):
    return [part for part in re.split("(?<!\.)\.(?!\.)", s)]


def args_to_dict(_args):
    _d = dictset()
    keys = _args.keys()
    # keys.sort()

    for arg in keys:
        value = _args[arg]

        bits = dot_split(arg)
        ctx = _d

        for i in range(len(bits)):
            bit = bits[i]
            last = not (i < len(bits) - 1)

            next_is_dict = False
            if not last:
                try:
                    int(bits[i + 1])
                except ValueError:
                    next_is_dict = True

            if isinstance(ctx, dict):
                if not ctx.has_key(bit):
                    if not last:
                        ctx[bit] = dictset() if next_is_dict else []
                        ctx = ctx[bit]
                    else:
                        ctx[bit] = type_cast(value)
                        ctx = None
                else:
                    ctx = ctx[bit]
            elif isinstance(ctx, list):
                if not last:
                    int_bit = int(bit)
                    if int_bit > len(ctx) - 1:
                        ctx.append(dictset() if next_is_dict else [])
                    try:
                        ctx = ctx[int_bit]
                    except IndexError as e:
                        pass
                else:
                    ctx.append(type_cast(value))
                    ctx = None
    return _d

#TODO: replace dict_to_args with this
import collections
def flat(_dict, parent_key='', sep='.', keep_lists=False, depth=-1):
    items = []

    if depth != -1:
        if depth == 0:
            return _dict
        depth -= 1

    for k, v in _dict.items():
        new_key = parent_key + sep + k if parent_key else k

        if isinstance(v, collections.MutableMapping):
            items.extend(flat(v, new_key, sep=sep,
                         keep_lists=keep_lists,
                         depth=depth).items())

        elif isinstance(v, collections.MutableSequence) and not keep_lists:
            for ix in range(len(v)):
                new_lkey = new_key + sep + str(ix)
                if isinstance(v[ix],
                        (collections.MutableSequence, collections.MutableMapping)):
                    items.extend(flat(v[ix], new_lkey, sep=sep, depth=depth).items())
                else:
                    items.append((new_lkey, v[ix]))
        else:
            items.append((new_key, v))

    return OrderedDict(items)


