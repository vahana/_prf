import urllib
from urlparse import urlparse
from prf.json_httpexceptions import *
from prf.utils import issequence

def obj2dict(request, result, fields=None):

    if isinstance(result, dict):
        return result

    fields = fields or []

    if hasattr(result, 'to_dict'):
        return result.to_dict(fields=fields, request=request)
    elif issequence(result):

        # make sure its mutable, i.e list
        result = list(result)
        for ix, each in enumerate(result):
            result[ix] = obj2dict(request, each, fields=fields)

    return result


def wrap_in_dict(request, result, fields=None):
    fields = fields or []

    if hasattr(result, '_prf_meta'):
        _meta = result._prf_meta
        fields = _meta.get('fields', [])
    else:
        _meta = {}

    result = obj2dict(request, result, fields=fields)

    if isinstance(result, dict):
        return result
    else:
        result = {'data': result}
        result.update(_meta)

    result = add_meta(request, result)
    return result


def add_meta(request, result):
    try:
        for each in result['data']:
            try:
                url = urlparse(request.current_route_url())._replace(query='')
                each.setdefault('self', '%s/%s' % (url.geturl(),
                                urllib.quote(str(each['id']))))
            except TypeError:
                pass
    except (TypeError, KeyError):
        pass
    finally:
        return result


def add_confirmation_url(request, result):
    q_or_a = ('&' if request.params else '?')
    return dict(method=request.method, count=len(result),
                confirmation_url=request.url + '%s__confirmation&_m=%s'
                % (q_or_a, request.method))


def wrap_in_http_created(request, result):
    if not result:
        return JHTTPCreated()

    return JHTTPCreated(location=request.current_route_url(result['id']),
                        resource=result)


def wrap_in_http_ok(request, result):
    return JHTTPOk()
