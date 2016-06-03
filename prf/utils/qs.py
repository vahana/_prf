import logging
from bson import ObjectId

from prf.utils import dictset, process_limit, split_strip

log = logging.getLogger(__name__)

def prep_params(params):

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
    )

    specials._sort = params.aslist('_sort', default=[], pop=True)
    specials._fields = params.aslist('_fields', default=[], pop=True)
    specials._flat = '_flat' in params; params.pop('_flat', False)
    specials._count = '_count' in params; params.pop('_count', False)
    specials._explain = '_explain' in params; params.pop('_explain', False)
    specials._start, specials._limit = process_limit(
                                        params.pop('_start', None),
                                        params.pop('_page', None),
                                        params.pop('_limit', 1))

    specials._ix = params.asint('_ix', pop=True, allow_missing=True, _raise=False)
    specials._end = specials._start+specials._limit\
                         if specials._limit > -1 else None

    specials._asdict = params.pop('_asdict', False)

    for each in params.keys():
        if each.startswith('_'):
            specials[each] = params.pop(each)


    params = typecast(params)

    return params, specials


def typecast(params):
    params = dictset(params)

    list_ops = ('in', 'nin', 'all')
    int_ops = ('exists', 'size')
    types = ('asbool', 'asint', 'asstr', 'aslist', 'asset', 'asdt', 'asobj')

    for key in params.keys():
        if params[key] == 'null':
            params[key] = None
            continue

        parts = key.split('__')
        if len(parts) <= 1:
            continue

        suf = []
        op = ''

        for ix in xrange(len(parts)-1, -1, -1):
            part = parts[ix]
            if part in list_ops+int_ops+types:
                op = part
                break

        if not op:
            continue

        new_key = '__'.join([e for e in parts if e != op])

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
            if callable(method):
                params[new_key] = method(key, pop=True)
        except (KeyError, AttributeError) as e:
            raise dictset.DValueError('Unknown typecast operator `%s`' % op)

    return params
