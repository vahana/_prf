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

from slovar.strings import split_strip, str2dt, str2rdt

from prf.utils.errors import DValueError

log = logging.getLogger(__name__)


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


def resolve(name, module=None):
    """Resole dotted name to python module
    """
    name = name.split('.')
    if not name[0]:
        if module is None:
            raise DValueError('relative name without base module')
        module = module.split('.')
        name.pop(0)
        while not name[0]:
            module.pop()
            name.pop(0)
        name = module + name

    used = name.pop(0)
    found = __import__(used)
    for n in name:
        used += '.' + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)

    return found


def maybe_dotted(module, throw=True):

    def _import(module):
        if isinstance(module, str):
            module, _, cls = module.partition(':')
            module = resolve(module)
            if cls:
                return getattr(module, cls)

        return module

    if throw:
        return _import(module)
    else:
        try:
            return _import(module)
        except ImportError as e:
            log.error('%s not found. %s' % (module, e))


def prep_params(params):
    from prf.utils import dictset

    specials = dictset(
        _sort=None,
        _fields=None,
        _count=None,
        _start=None,
        _limit=None,
        _page=None,
        _frequencies=None,
        _group=None,
        _distinct=None,
        _scalar=None,
        _ix=None,
        _explain=None,
        _flat=None,
        _join=None,
        _unwind=None,
        _where=None,
    )

    specials._sort = params.aslist('_sort', default=[], pop=True)
    specials._fields = params.aslist('_fields', default=[], pop=True)
    specials._flat = '_flat' in params; params.pop('_flat', False)
    specials._count = '_count' in params; params.pop('_count', False)
    specials._explain = '_explain' in params; params.pop('_explain', False)

    specials._start, specials._limit = process_limit(
                                        params.pop('_start', None),
                                        params.pop('_page', None),
                                        params.asint('_limit', pop=True))

    specials._ix = params.asint('_ix', pop=True, allow_missing=True, _raise=False)
    specials._end = specials._start+specials._limit\
                         if specials._limit > -1 else None

    specials._asdict = params.pop('_asdict', False)

    for each in list(params.keys()):
        if each.startswith('_'):
            specials[each] = params.pop(each)

        if '.' in each:
            params[each.replace('.', '__')] = params.pop(each)

    params = typecast(params)

    if specials._where:
        params['__raw__'] = {'$where': specials._where}

    return params, specials


def typecast(params):
    from prf.utils import dictset

    params = dictset(params)

    list_ops = ('in', 'nin', 'all')
    int_ops = ('exists', 'size', 'max_distance', 'min_distance', 'empty')
    geo_ops = ('near',)
    types = ('asbool', 'asint', 'asfloat', 'asstr', 'aslist',
                'asset', 'asdt', 'asobj')


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
    new_d = dict()

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


def extract_domain(url, _raise=True):
    import tldextract
    try:
        parsed = tldextract.extract(url)
        domain = parsed.registered_domain
        subdomain = parsed.subdomain

        if subdomain.startswith('www'):
            #ie [www].abc.com
            subdomain = subdomain[3:]
            if subdomain.startswith('.'):
                #ie [www.abc].xyz.com
                subdomain = subdomain[1:]

        if subdomain:
            domain = '%s.%s' % (subdomain, domain)

        return domain

    except TypeError as e:
        if _raise:
            raise DValueError(e)

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

def clean_postal_code(code):
    return code.partition('-')[0]

def format_phone(number, country_code, _raise=True):
    import phonenumbers as pn

    try:
        phone = pn.parse(number, country_code)
        ok = True
        if not pn.is_possible_number(phone):
            msg = 'Phone number `%s` for country `%s` might might be invalid'\
                                 % (number, country_code)
            if _raise:
                raise DValueError(msg)
            else:
                log.warn(msg)
                ok = False

        return ok, pn.format_number(phone,
                        pn.PhoneNumberFormat.INTERNATIONAL)

    except pn.NumberParseException as e:
        if _raise:
            raise DValueError(e)

    return False, None

def normalize_phone(number, country_code='US', _raise=True):
    import phonenumbers as pn

    try:
        phone = pn.parse(number, country_code)
        if not pn.is_valid_number(phone) and _raise:
                raise DValueError('Invalid phone number `%s` for country `%s`'
                                    % (number, country_code))
        else:
            return None

        return str(phone.national_number)

    except pn.NumberParseException as e:
        if _raise:
            raise DValueError(e)


def dl2ld(dl):
    "dict of lists to list of dicts"

    return [{key:value[index] for key, value in list(dl.items())}
            for index in range(len(list(dl.values())[0]))]

def ld2dd(ld, key):
    'list of dicts to dict of dicts'
    return {each[key]:each for each in ld}


def qs2dict(qs):
    from urllib.parse import parse_qsl
    from prf.utils import dictset
    return dictset(parse_qsl(qs,keep_blank_values=True))


def TODAY():
    return datetime.now().strftime('%Y_%m_%d')

def NOW():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def raise_or_log(_raise=False):
    if _raise:
        import sys
        _type, value, _ = sys.exc_info()
        raise _type(value)
    else:
        import traceback
        traceback.print_exc()
