import logging
import json

import mongoengine as mongo
from pyramid.config import Configurator
from prf.renderers import _JSONEncoder

log = logging.getLogger(__name__)


def json_dumps(body):
    return json.dumps(body, cls=_JSONEncoder)


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
    except mongo.InvalidQueryError, e:

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


def maybe_dotted(modul, throw=True):
    '''if ``modul`` is a dotted string pointing to the modul, imports and returns the modul object.'''
    try:
        return Configurator().maybe_dotted(modul)
    except ImportError, e:
        err = '%s not found. %s' % (modul, e)
        if throw:
            raise ImportError(err)
        else:
            log.error(err)
            return None


def get_document_cls(name):
    try:
        return mongo.document.get_document(name)
    except Exception, e:
        raise ValueError('`%s` does not exist in mongo db' % name)
