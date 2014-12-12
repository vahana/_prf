import os
import re
import logging
from contextlib import contextmanager
import json

import mongoengine as mongo
from pyramid.config import Configurator

from prf.renderers import _JSONEncoder

log = logging.getLogger(__name__)


def json_dumps(body):
    return json.dumps(body, cls=_JSONEncoder)


def split_strip(_str, on=','):
    lst = _str if isinstance(_str, list) else _str.split(on)
    return filter(bool, [e.strip() for e in lst])


def process_limit(start, page, limit):
    try:
        limit = int(limit)

        if start is not None and page is not None:
            raise ValueError(
                'Can not specify _start and _page at the same time')

        if start is not None:
            start = int(start)
        elif page is not None:
            start = int(page) * limit
        else:
            start = 0

        if limit < 0 or start < 0:
            raise ValueError('_limit/_page or _limit/_start can not be < 0')

    except (ValueError, TypeError) as e:
        raise ValueError(e)

    except mongo.InvalidQueryError as e:
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


def process_fields(_fields):
    fields_only = []
    fields_exclude = []

    if isinstance(_fields, basestring):
        _fields = split_strip(_fields)

    for field in extend_list(_fields):
        field = field.strip()
        if not field:
            continue
        if field[0] == "-":
            fields_exclude.append(field[1:])
        else:
            fields_only.append(field)
    return fields_only, fields_exclude


def snake2camel(text):
    "turn the snake case to camel case: snake_camel -> SnakeCamel"
    return ''.join([a.title() for a in text.split("_")])


def maybe_dotted(modul, throw=True):
    "if ``modul`` is a dotted string pointing to the modul, imports and returns the modul object."
    try:
        return Configurator().maybe_dotted(modul)
    except ImportError, e:
        err = '%s not found. %s' % (modul, e)
        if throw:
            raise ImportError(err)
        else:
            log.error(err)
            return None


@contextmanager
def chdir(path):
    old_dir = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(old_dir)


def isnumeric(value):
    """Return True if `value` can be converted to a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def issequence(arg):
    """Return True if `arg` acts as a list and does not look like a string."""
    return (not hasattr(arg, 'strip') and hasattr(arg, '__getitem__') or
            hasattr(arg, '__iter__'))


def get_document_cls(name):
    try:
        return mongo.document.get_document(name)
    except Exception as e:
        raise ValueError('`%s` does not exist in mongo db' % name)
