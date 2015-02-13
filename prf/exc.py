import logging
import traceback
from datetime import datetime
import uuid
from pyramid import httpexceptions as http_exc

from prf.utils import dictset, json_dumps

logger = logging.getLogger(__name__)

def add_stack():
    return ''.join(traceback.format_stack())

def is_error(status_code):
    return 400 <= status_code < 600

def log_exception(resp, params):

    if is_error(resp.status_code) and resp.status_code != 404:
        msg = '%s: %s' % (resp.status.upper(), json_dumps(params))
        if resp.status_int in [400, 500]:
            msg += '\nSTACK BEGIN>>\n%s\nSTACK END<<' % add_stack()
        logger.error(msg)

def create_response(resp, params, **extra):
    resp.content_type = 'application/json'

    body = dict(
        code = resp.code,
        title = resp.title,
        explanation = resp.explanation,
        detail = resp.detail,
    )
    body.update(extra)

    if is_error(resp.status_code):
        body['id'] = uuid.uuid4()
        params['timestamp'] = datetime.utcnow()

    params.update(body)
    log_exception(resp, params)

    resp.body = json_dumps(body)
    return resp

def exception_response(status_code, **kw):
    if status_code == 400:
        return HTTPBadRequest(**kw)

    return create_response(http_exc.exception_response(status_code), kw)

# 20x
def HTTPOk(*arg, **kw):
    return create_response(http_exc.HTTPOk(*arg), kw)

def HTTPCreated(*arg, **kw):
    resource = kw.pop('resource', None)
    if resource and 'location' in kw:
        resource['self'] = kw['location']

    resp = create_response(http_exc.HTTPCreated(*arg), kw, resource=resource)
    return resp

# 30x
def HTTPFound(*arg, **kw):
    return create_response(http_exc.HTTPFound(*arg), kw)

# 40x
def HTTPNotFound(*arg, **kw):
    return create_response(http_exc.HTTPNotFound(*arg), kw)

def HTTPUnauthorized(*arg, **kw):
    return create_response(http_exc.HTTPUnauthorized(*arg), kw)

def HTTPForbidden(*arg, **kw):
    return create_response(http_exc.HTTPForbidden(*arg), kw)

def HTTPConflict(*arg, **kw):
    return create_response(http_exc.HTTPConflict(*arg), kw)

def HTTPBadRequest(*arg, **kw):
    return create_response(http_exc.HTTPBadRequest(*arg), kw)

def HTTPMethodNotAllowed(*arg, **kw):
    return create_response(http_exc.HTTPMethodNotAllowed(*arg), kw)

# 50x
def HTTPServerError(*arg, **kw):
    return create_response(http_exc.HTTPServerError(*arg), kw)

def HTTPInternalServerError(*arg, **kw):
    return create_response(http_exc.HTTPInternalServerError(*arg), kw)

def HTTPNotImplemented(*arg, **kw):
    return create_response(http_exc.HTTPNotImplemented(*arg), kw)

def HTTPServiceUnavailable(*arg, **kw):
    return create_response(http_exc.HTTPServiceUnavailable(*arg), kw)
