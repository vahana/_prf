import logging
import requests
import urllib

from prf.utils.utils import json_dumps
from prf.utils import dictset
import prf.exc

log = logging.getLogger(__name__)


def pyramid_resp(resp, **kw):
    from pyramid.response import Response
    return Response(status_code=resp.status_code, headers=resp.headers,
                    body=resp.text, **kw)


class Requests(object):

    def __init__(self, base_url=''):
        self.base_url = base_url

    def json_body(self, resp):
        try:
            return dictset(resp.json())
        except:
            log.error('Response does not contain json body')
            return {}

    def prepare_url(self, path='', params={}):
        url = self.base_url

        if not url:
            url = path
        elif path:
            url = '%s%s' % (url, (path if path.startswith('/') else '/'
                            + path))

        if params:
            url = '%s%s%s' % (url, ('&' if '?' in url else '?'),
                              urllib.urlencode(params))

        return url

    def get(self, path, params={}, **kw):
        url = self.prepare_url(path, params)
        log.debug('%s', url)

        try:
            resp = requests.get(url, **kw)
            if not resp.ok:
                raise prf.exc.exception_response(**self.json_body(resp))
            return self.json_body(resp)

        except requests.ConnectionError, e:
            raise prf.exc.HTTPGatewayTimeout('Could not reach %s' % e.request.url)

    def mget(self, path, params={}, page_size=None):
        total = params['_limit']
        start = params.get('_start', 0)
        params['_limit'] = page_size
        page_count = total / page_size

        for ix in range(page_count):
            params['_start'] = start + ix * page_size
            yield self.get(path, params)

        reminder = total % page_size
        if reminder:
            params['_start'] = start + page_count * page_size
            params['_limit'] = reminder
            yield self.get(path, params)

    def post(self, path='', data={}, **kw):
        url = self.prepare_url(path)
        log.debug('%s, kwargs:%.512s', url, data)
        try:
            resp = requests.post(url, data=json_dumps(data),
                                 headers={'content-type': 'application/json'},
                                 **kw)
            if not resp.ok:
                raise prf.exc.exception_response(status_code=resp.status_code,
                                                 **self.json_body(resp))

            return pyramid_resp(resp)

        except requests.ConnectionError, e:
            raise prf.exc.HTTPGatewayTimeout('Could not reach %s' % e.request.url)

    def mpost(self, path='', data={}, bulk_size=None, bulk_key=None):
        bulk_data = data[bulk_key]
        total = len(bulk_data)
        page_count = total / bulk_size

        for ix in range(page_count):
            data[bulk_key] = bulk_data[ix * bulk_size:(ix + 1) * bulk_size]
            yield self.post(path, data)

        reminder = total % bulk_size
        if reminder:
            st = page_count * bulk_size
            data[bulk_key] = bulk_data[st:st + reminder]
            yield self.post(path, data)

    def put(self, path='', data={}, **kw):
        try:
            url = self.prepare_url(path)
            log.debug('%s, kwargs:%.512s', url, data)

            resp = requests.put(url, data=json_dumps(data),
                                headers={'content-type': 'application/json'},
                                **kw)
            if not resp.ok:
                raise prf.exc.exception_response(status_code=resp.status_code,
                                                 **self.json_body(resp))

            return dictset(resp.json())

        except requests.ConnectionError, e:
            raise prf.exc.HTTPGatewayTimeout('Could not reach %s' % e.request.url)

    def head(self, path='', params={}):
        try:
            resp = requests.head(self.prepare_url(path, params))
            if not resp.ok:
                raise prf.exc.exception_response(status_code=resp.status_code,
                                                 **self.json_body(resp))

        except requests.ConnectionError, e:
            raise prf.exc.HTTPGatewayTimeout('Could not reach %s' % e.request.url)

    def delete(self, path='', **kw):
        url = self.prepare_url(path)
        log.debug(url)
        try:
            resp = requests.delete(url,
                                   headers={'content-type': 'application/json'
                                   }, **kw)
            if not resp.ok:
                raise prf.exc.exception_response(status_code=resp.status_code,
                                                 **self.json_body(resp))

            return dictset(resp.json())

        except requests.ConnectionError, e:
            raise prf.exc.HTTPGatewayTimeout('Could not reach %s' % e.request.url)
