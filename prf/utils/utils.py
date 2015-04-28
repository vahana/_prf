import json
import logging
from urlparse import urlparse, parse_qs
from urllib import urlencode
from datetime import date, datetime

log = logging.getLogger(__name__)

class DKeyError(KeyError):
    pass


class DValueError(ValueError):
    pass


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')  # iso

        try:
            return super(JSONEncoder, self).default(obj)
        except TypeError:
            return unicode(obj)  # fallback to unicode


def json_dumps(body):
    return json.dumps(body, cls=JSONEncoder)


def split_strip(_str, on=','):
    lst = (_str if isinstance(_str, list) else _str.split(on))
    return filter(bool, [e.strip() for e in lst])


def process_limit(start, page, limit):
    try:
        limit = int(limit)

        if start is not None and page is not None:
            raise ValueError('Can not specify _start and _page at the same time'
                             )

        if start is not None:
            start = int(start)
        elif page is not None:
            start = int(page) * limit
        else:
            start = 0

        if limit < 0 or start < 0:
            raise DValueError('_limit/_page or _limit/_start can not be < 0')
    except (ValueError, TypeError), e:
        raise DValueError(e)
    except Exception, e: #pragma nocover
        raise DValueError('Bad _limit param: %s ' % e)

    return start, limit


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


def process_fields(fields):
    fields_only = []
    fields_exclude = []

    if isinstance(fields, basestring):
        fields = split_strip(fields)

    for field in expand_list(fields):
        field = field.strip()
        if not field:
            continue
        if field[0] == '-':
            fields_exclude.append(field[1:])
        else:
            fields_only.append(field)
    return fields_only, fields_exclude


def snake2camel(text):
    '''turn the snake case to camel case: snake_camel -> SnakeCamel'''
    return ''.join([a.title() for a in text.split('_')])


def resolve(name, module=None):
    """Resole dotted name to python module
    """
    name = name.split('.')
    if not name[0]:
        if module is None:
            raise ValueError('relative name without base module')
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
        if isinstance(module, basestring):
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
        except ImportError, e:
            log.error('%s not found. %s' % (module, e))


def issequence(arg):
    """Return True if `arg` acts as a list and does not look like a string."""
    return not hasattr(arg, 'strip') and hasattr(arg, '__getitem__') \
        or hasattr(arg, '__iter__')


def prep_params(params):
    # import here to avoid circular import
    from prf.utils import dictset

    specials = dictset()

    specials._sort = split_strip(params.pop('_sort', []))
    specials._fields = split_strip(params.pop('_fields', []))
    specials._count = '_count' in params
    params.pop('_count', None)

    _limit = params.pop('_limit', 1)
    _page = params.pop('_page', None)
    _start = params.pop('_start', None)

    specials._offset, specials._limit = process_limit(_start, _page, _limit)

    return dictset(params), specials


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


def normalize_domain(url):
    if not url:
        return url

    elements = urlparse(url)
    domain = elements.netloc if elements.scheme else elements.path
    return domain.split('www.')[-1]


def resolve_host_to(url, newhost):
    elements = urlparse(url)
    _, _, port = elements.netloc.partition(':')
    if port:
        newhost = '%s:%s' % (newhost, port)
    return elements._replace(netloc=newhost).geturl()


def sanitize_url(url, to_remove=None):
    if not to_remove:
        return urlparse(url)._replace(query='').geturl()

    if isinstance(to_remove, basestring):
        to_remove = [to_remove]

    elements = urlparse(url)
    qs_dict = parse_qs(elements.query)
    for rm in to_remove:
        qs_dict.pop(rm, None)

    return elements._replace(
        query=urlencode(qs_dict, True)).geturl()
