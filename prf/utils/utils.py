import re
import json
import dateutil
import logging
import urllib3
import collections
from bson import ObjectId

from urllib.parse import urlparse, parse_qs, parse_qsl
from datetime import date, datetime
import requests
from functools import partial
from time import time, sleep

from slovar.strings import split_strip, str2dt, str2rdt
from slovar.utils import maybe_dotted
from slovar import slovar

from prf.utils.errors import DValueError, DKeyError

log = logging.getLogger(__name__)

OPERATORS = ['ne', 'lt', 'lte', 'gt', 'gte', 'in', 'all',
             'startswith', 'exists', 'range', 'geobb', 'size']


class Params(slovar):
    'Subclass of slovar that will raise D* exceptions'
    def __init__(self, *arg, **kw):
        super().__init__(*arg, **kw)

    def bad_value_error_klass(self, e):
        return DValueError(e)

    def missing_key_error_klass(self, e):
        return DKeyError(e)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat().split(".")[0]
        try:
            return super(JSONEncoder, self).default(obj)
        except TypeError:
            return str(obj)  # fallback to unicode


def json_dumps(body):
    return json.dumps(body, cls=JSONEncoder)


def process_limit(start, page, limit):
    try:
        limit = int(limit)

        if start is not None and page is not None:
            raise DValueError('Can not specify _start and _page at the same time')

        if start is not None:
            start = int(start)
        elif page is not None and limit > 0:
            start = int(page) * limit
        else:
            start = 0

        if limit < -1 or start < 0:
            raise DValueError('_limit/_page or _limit/_start can not be < 0')
    except (ValueError, TypeError) as e:
        raise DValueError(e)
    except Exception as e: #pragma nocover
        raise DValueError('Bad _limit param: %s ' % e)

    return start, limit


def snake2camel(text):
    '''turn the snake case to camel case: snake_camel -> SnakeCamel'''
    return ''.join([a.title() for a in text.split('_')])


def camel2snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def process_key(key, suffix=''):
    _key, div, op = key.rpartition('__')
    if div and op in OPERATORS:
        key = _key
    key = key.replace('__', '.')
    return ('%s.%s' % (key, suffix) if suffix else key), (op if op in OPERATORS else '')


def parse_specials(orig_params):
    specials = Params(
        _sort=None,
        _fields=None,
        _count=None,
        _start=None,
        _limit=None,
        _page=None,
        _end=None,
        _frequencies=None,
        _group=None,
        _distinct=None,
        _scalar=None,
        _ix=None,
        _explain=None,
        _flat=None,
        _flat_sep=None,
        _flat_keep_lists=None,
        _join=None,
        _unwind=None,
        _where=None,
        _or=None,
        _type=None,
        _meta=None
    )

    def short(name):
        _n = name[:2]
        if _n in params:
            return _n
        else:
            return name

    params = orig_params.copy()

    specials._sort = params.aslist(short('_sort'), default=[], pop=True)
    specials._fields = params.aslist(short('_fields'), default=[], pop=True)
    specials._flat = params.aslist(short('_flat'), default=[], pop=True)
    specials._group = params.aslist(short('_group'), default=[], pop=True)

    specials._count = short('_count') in params; params.pop(short('_count'), False)

    specials._explain = '_explain' in params; params.pop('_explain', False)

    params.asint('_start', allow_missing=True)
    params.asint('_page', allow_missing=True)
    params.asint('_limit', allow_missing=True)

    if not specials._count and params.get('_limit'):
        specials._start, specials._limit = process_limit(
                                        params.pop('_start', None),
                                        params.pop('_page', None),
                                        params.asint(short('_limit'), pop=True))

        specials._end = specials._start+specials._limit\
                         if specials._limit > -1 else None

    specials._ix = params.asint('_ix', pop=True, allow_missing=True, _raise=False)
    specials._flat_keep_lists = params.asbool('_flat_keep_lists', default=False)
    specials._flat_sep = params.asstr('_flat_sep', default='.')

    specials._asdict = params.pop('_asdict', False)
    specials._pop_empty = params.pop('_pop_empty', False)

    for each in list(params.keys()):
        if each.startswith('_'):
            specials[each] = params.pop(each)

        if '.' in each and each in params:
            params[each.replace('.', '__')] = params.pop(each)

    params = typecast(params)

    if specials._where:
        params['__raw__'] = {'$where': specials._where}

    if specials._type:
        field,_type = specials._type.split(':')
        params['__raw__'] = {field:{'$type':_type}}

    if 'AUTO' in specials._fields:
        for kk in params:
            fld, _ = process_key(kk)
            specials._fields.append(fld)

    specials._meta = params.get('_meta', slovar())

    return params, specials


def typecast(params):
    params = Params(params)

    list_ops = ('in', 'nin', 'all')
    int_ops = ('exists', 'size', 'max_distance', 'min_distance', 'empty')
    geo_ops = ('near',)
    types = ('asbool', 'asint', 'asfloat', 'asstr', 'aslist',
                'asset', 'asdt', 'asobj', 'asdtob')


    for key in list(params.keys()):
        if params[key] == 'null':
            params[key] = None
            continue

        parts = key.split('__')
        if len(parts) <= 1:
            continue

        suf = []
        op = ''

        for ix in range(len(parts)-1, -1, -1):
            part = parts[ix]
            if part in list_ops+int_ops+geo_ops+types:
                op = part
                break

        if not op:
            continue

        new_key = '__'.join([e for e in parts if e != op])

        if op in geo_ops:
            coords = params.aslist(key)

            try:
                coords = [float(e) for e in coords]
                if len(coords) != 2:
                    raise ValueError

            except ValueError:
                raise DValueError('`near` operator takes pair of'
                                ' numeric elements. Got `%s` instead' % coords)

            params[key] = coords
            continue

        if op in list_ops:
            new_key = key
            op = 'aslist'

        if op in int_ops:
            new_key = key
            op = 'asint'

        if not op.startswith('as') and not op in types:
            continue

        if op == 'asobj':
            params[new_key]=ObjectId(params.pop(key))
            continue

        try:
            method = getattr(params, op)
            if isinstance(method, collections.Callable):
                params[new_key] = method(key, pop=True)
        except (KeyError, AttributeError) as e:
            raise DValueError('Unknown typecast operator `%s`' % op)

    return params


def with_metaclass(meta, *bases):
    """Defines a metaclass.

    Creates a dummy class with a dummy metaclass. When subclassed, the dummy
    metaclass is used, which has a constructor that instantiates a
    new class from the original parent. This ensures that the dummy class and
    dummy metaclass are not in the inheritance tree.

    Credit to Armin Ronacher.
    """
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__
        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})


def resolve_host_to(url, newhost):
    '''
    substitute the host in `url` with `newhost`
    if newhost ends with `:` the original port will be preserved.
    '''

    elements = urlparse(url)
    _, _, port = elements.netloc.partition(':')
    newhost,newcol,newport=newhost.partition(':')

    if newcol:
        if not newport:
            newport = port
    else:
        newport = ''

    if newport:
        newhost = '%s:%s' % (newhost, newport)
    return elements._replace(netloc=newhost).geturl()


def sanitize_url(url, to_remove=None):
    if not to_remove:
        return urlparse(url)._replace(query='').geturl()

    if isinstance(to_remove, str):
        to_remove = [to_remove]

    elements = urlparse(url)
    qs_dict = parse_qs(elements.query)
    for rm in to_remove:
        qs_dict.pop(rm, None)

    return elements._replace(
        query=urlencode(qs_dict, True)).geturl()

def to_dunders(d, only=None):
    new_d = slovar()

    for key in d:
        if only and key not in only:
            continue
        if '__' not in key:
            new_d['set__%s'%key.replace('.', '__')] = d[key]
        else:
            new_d[key] = d[key]

    return new_d


def validate_url(url, method='GET'):
    from requests import Session, Request
    try:
        return Session().send(Request(method, url).prepare()).status_code
    except Exception:
        raise DValueError('URL not reachable `%s`' % url)


def is_url(text, validate=False):
    if text.startswith('http'):
        if validate:
            return validate_url(text)
        else:
            return True
    return False


def chunks(_list, chunk_size):
    for ix in range(0, len(_list), chunk_size):
        yield _list[ix:ix+chunk_size]


def encoded_dict(in_dict):
    out_dict = {}
    for k, v in list(in_dict.items()):

        if isinstance(v, dict):
            out_dict[k] = encoded_dict(v)
        elif isinstance(v, list):
            for ix in range(len(v)):
                v[ix] = str(v[ix]).encode('utf-8')
            out_dict[k] = v
        else:
            out_dict[k] = str(v).encode('utf-8')

    return out_dict


def urlencode(query, doseq=False):
    import urllib.request, urllib.parse, urllib.error
    try:
        return urllib.parse.urlencode(encoded_dict(query), doseq)
    except UnicodeEncodeError as e:
        log.error(e)


def pager(start, page, total):
    def _pager(start,page,total):
        if total != -1:
            for each in chunks(list(range(0, total)), page):
                _page = len(each)
                yield (start, _page)
                start += _page
        else:
            while 1:
                yield (start, page)
                start += page

    return partial(_pager, start, page, total)


def cleanup_url(url, _raise=True):
    if not url:
        if _raise:
            raise DValueError('bad url `%s`' % url)
        return ''

    try:
        parsed = urllib3.util.parse_url(url)
    except Exception as e:
        if _raise:
            raise e
        return ''

    host = parsed.host

    if not host:
        if _raise:
            raise DValueError('missing host in %s' % url)
        else:
            return ''

    if host.startswith('www.'):
        host = host[4:]

    path = (parsed.path or '').strip('/')
    return ('%s/%s' % (host, path)).strip('/')


def ld2dl(ld):
    dl = {}
    for each in ld:
        for kk,vv in each:
            if kk in dl:
                dl[kk].append(vv)
            else:
                dl[kk] = [vv]
    return dl


def dl2ld(dl):
    "dict of lists to list of dicts"

    return [{key:value[index] for key, value in list(dl.items())}
            for index in range(len(list(dl.values())[0]))]

def ld2dd(ld, key):
    'list of dicts to dict of dicts'
    return {each[key]:each for each in ld}

def d2inv(d, value_as_list=True):
    inv_dict = {}

    for kk,vv in d.items():
        if value_as_list:
            inv_dict[vv] = inv_dict.get(vv, [])
            inv_dict[vv].append(kk)
        elif vv not in inv_dict:
            inv_dict[vv] = kk

    return inv_dict

def qs2dict(qs):
    from urllib.parse import parse_qsl
    return slovar(parse_qsl(qs,keep_blank_values=True))

def TODAY(sep='_'):
    return datetime.utcnow().strftime(sep.join(['%Y', '%m', '%d']))

def NOW(sep=None):
    dnow = datetime.utcnow()
    if sep:
        return dnow.strftime(sep.join(['%Y', '%m', '%dT%H', '%M', '%S']))
    return dnow.strftime('%Y-%m-%dT%H:%M:%S')

def raise_or_log(_raise=False):
    if _raise:
        import sys
        _type, value, _ = sys.exc_info()
        raise _type(value)
    else:
        import traceback
        traceback.print_exc()


def join(objects, joinee_itor, join_on, require_match=True, join_ns=None,
                                            join_unwind=True, join_params=None):
    '''
        require_match = False is equivalent to SQL left join
        require_match = True is equivalent to SQL left inner join
    '''

    join_params = slovar(join_params or {}).flat()

    for each in objects:
        _d1 = each.to_dict()

        join_params.update(_d1.extract(join_on).flat())

        if not join_params:
            if require_match:
                log.warning('empty query for cls2')
                continue
            else:
                yield _d1
                continue

        join_params.setdefault('_limit', 1)
        matched = joinee_itor(**join_params)

        if not matched:
            if require_match:
                continue
            elif not join_ns:
                yield _d1
                continue
            else:
                matched = [slovar()] #attach empty slovar to results as join_ns

        if not join_unwind:
            joinee_list = [it.to_dict(join_params.get('_fields', [])) for it in matched]
            _d1[join_ns or 'joinee'] = joinee_list
            yield _d1

        else:
            for it in matched:
                _d2 = it.to_dict(join_params.get('_fields', []))

                if join_ns:
                    _d2 = slovar({join_ns:_d2})

                yield _d1.update_with(_d2)


def rextract(expr, data, delim, _raise=True):
    result = re.search('%s(.*)%s' % (delim, delim), expr)
    if result:
        fld = result.group(1)
        val = data.extract(fld)

        if val:
            return expr.replace(result.group(0), typecast(val.flat())[fld])

        else:
            msg = 'missing fields in data.\nfields: %s\ndata keys: %s' % (fld, data.keys())
            if _raise:
                raise ValueError(msg)
            else:
                log.error(msg)


class Throttler:
    def __init__(self, max_counter, period):
        self.max_counter = max_counter
        self.period = period
        self.reset()

    def reset(self):
        self.stime = time()
        self.counter = 0

    def pause(self):
        time_past = time() - self.stime
        sleep_for = self.period-time_past
        log.debug('THROTTLE: sleep for %s, with %s' % (sleep_for, self.__dict__))
        sleep(sleep_for)
        self.reset()

    def __call__(self):
        self.counter += 1
        if time() - self.stime <= self.period and self.counter >= self.max_counter:
            self.pause()
