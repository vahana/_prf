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
    #exclude 404 not found
    return (400 <= status_code < 600) and status_code != 404


def log_exception(resp, params):
    msg = '%s: %s' % (resp.status.upper(), json_dumps(params))
    if resp.status_code in [400, 500]:
        msg += '\nSTACK BEGIN>>\n%s\nSTACK END<<' % add_stack()

    logger.error(msg)


def create_response(resp, params):
    resp.content_type = 'application/json'
    resp.headers.extend(params.pop('headers', []))

    extra = params.pop('extra', {})

    request = params.pop('request', None)
    if request and not isinstance(request, dict):
        request = dict(
                method = request.method,
                url = request.url,
                remote_user = request.remote_user,
                client_addr = request.client_addr,
                remote_addr = request.remote_addr,
            )

    params.update(
        dict(
            code = resp.code,
            title = resp.title,
            explanation = resp.explanation,
            detail = resp.detail or params.get('detail', ''),
            timestamp = datetime.utcnow()
    ))

    params.update(extra)

    if is_error(resp.status_code):
        params['request'] = request
        params['error_id'] = uuid.uuid4()
        log_exception(resp, params)

    resp.body = json_dumps(params)
    return resp


def _raise(response):
    try:
        error = response.json()
    except:
        error = {'message': response.text}
    raise exception_response(status_code=response.status_code, **error)


def exception_response(status_code, **kw):
    # for some reason 400 is mapped to HTTPClientError in pyramid instead of HTTPBadRequest
    # lets map it manually here
    if status_code == 400:
        return HTTPBadRequest(kw.get('detail', ''), **kw)

    return create_response(http_exc.exception_response(status_code), kw)

# 20x
def HTTPOk(*arg, **kw):
    return create_response(http_exc.HTTPOk(*arg), kw)

def HTTPCreated(*arg, **kw):
    resource = kw.pop('resource', None)
    if resource and 'location' in kw:
        resource['self'] = kw['location']

    if 'extra' in kw:
        kw['extra']['resource'] = resource
    else:
        kw['extra'] = {'resource': resource}

    resp = create_response(http_exc.HTTPCreated(*arg), kw)
    return resp

# 30x
def HTTPFound(*arg, **kw):
    return create_response(http_exc.HTTPFound(*arg,
                            location=kw['location']), kw)

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

def HTTPNotAcceptable(*arg, **kw):
    return create_response(http_exc.HTTPNotAcceptable(*arg), kw)


# 50x
def HTTPServerError(*arg, **kw):
    return create_response(http_exc.HTTPServerError(*arg), kw)

def HTTPGatewayTimeout(*arg, **kw):
    return create_response(http_exc.HTTPGatewayTimeout(*arg), kw)

def HTTPInternalServerError(*arg, **kw):
    return create_response(http_exc.HTTPInternalServerError(*arg), kw)

def HTTPNotImplemented(*arg, **kw):
    return create_response(http_exc.HTTPNotImplemented(*arg), kw)

def HTTPServiceUnavailable(*arg, **kw):
    return create_response(http_exc.HTTPServiceUnavailable(*arg), kw)
