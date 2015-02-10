import json
import logging
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
            raise ValueError('_limit/_page or _limit/_start can not be < 0')
    except (ValueError, TypeError), e:
        raise ValueError(e)
    except Exception, e:
        raise ValueError('Bad _limit param: %s ' % e)

    return start, limit


def extend_list(param):
    _new = []
    if isinstance(param, (list, set)):
        for each in param:
            if isinstance(each, basestring) and each.find(',') != -1:
                _new.extend(split_strip(each))
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

    for field in extend_list(fields):
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
    try:
        if isinstance(module, basestring):
            module, _, cls = module.partition(':')
            module = resolve(module)
            if cls:
                return getattr(module, cls)

        return module
    except ImportError, e:

        err = '%s not found. %s' % (module, e)
        if throw:
            raise ImportError(err)
        else:
            log.error(err)
            return None

def issequence(arg):
    """Return True if `arg` acts as a list and does not look like a string."""
    return not hasattr(arg, 'strip') and hasattr(arg, '__getitem__') \
        or hasattr(arg, '__iter__')


def prep_params(params):
    # import here to avoid circular import
    from prf.utils import dictset

    __confirmation = '__confirmation' in params
    params.pop('__confirmation', False)

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
