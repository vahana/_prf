import logging
import requests
from urlparse import urljoin

from prf.utils.utils import json_dumps, urlencode
from prf.utils import dictset
import prf.exc

log = logging.getLogger(__name__)


def pyramid_resp(resp, **kw):
    from pyramid.response import Response
    return Response(status_code=resp.status_code, headers=resp.headers,
                    body=resp.text, **kw)


class PRFHTTPAdapter(requests.adapters.HTTPAdapter):
    def send(self, *args, **kw):
        try:
            return super(PRFHTTPAdapter, self).send(*args, **kw)
        except (requests.ConnectionError, requests.Timeout) as e:
            raise prf.exc.HTTPGatewayTimeout('%s for %s' % (str(e), e.request.url))


class Request(object):

    def __init__(self, base_url='', cache_options=None,
                      _raise=True,
                      delay=0, reqs_over_time = None,
                      cookies=None, headers=None):

        self.base_url = base_url
        self.cache_options = cache_options or {}
        self._raise = _raise
        self.delay = delay
        self.reqs_over_time = reqs_over_time or [] # [3,60] - 3 requests in 60 seconds

        if self.cache_options:
            import requests_cache
            self.session = requests_cache.CachedSession(**self.cache_options)
        else:
            self.session = requests.Session()

        self.session.mount('http://', PRFHTTPAdapter())
        self.session.mount('https://', PRFHTTPAdapter())

        self.session.headers['content-type'] =  'application/json'

        if cookies:
            self.session.cookies.update(cookies)

        if headers:
            self.session.headers.update(headers)

    def login(self, url, login, password):
        resp = Request(url).post(data={'login':login,
                            'password':password})

        self.session.cookies.update(resp.cookies)

    def json(self, resp):
        try:
            return dictset(resp.json())
        except:
            log.error('Response does not contain json body')
            return None

    def is_json(self, data):
        return isinstance(data, (tuple, list, dict)) \
            and self.session.headers['content-type'] == 'application/json'

    def raise_or_log(self, resp):
        if self._raise:
            params = self.json(resp) or {'description':resp.text}
            raise prf.exc.exception_response(status_code=resp.status_code,
                                    **params)

        log.error(str(self.json(resp)))
        return resp

    def from_cache(self, resp):
        return hasattr(resp, 'from_cache') and resp.from_cache

    def prepare_url(self, path='', params={}):
        url = self.base_url

        # if not url:
        #     url = path

        # elif path:
            # url = urljoin(url, path)

        url = urljoin(self.base_url, path)
        if params:
            url = '%s%s%s' % (url, ('&' if '?' in url else '?'), urlencode(params))

        return url

    def get(self, path='', params={}, **kw):
        url = self.prepare_url(path, params)
        log.debug('%s', url)

        resp = self.session.get(url, **kw)
        if not resp.ok:
            return self.raise_or_log(resp)

        return resp

    def multi_submit(self, reqs):
        from requests_throttler import BaseThrottler

        kwargs={}
        if self.delay:
            kwargs['delay'] = self.delay
        elif self.reqs_over_time:
            kwargs['reqs_over_time'] = self.reqs_over_time

        with BaseThrottler(name='throttler', session=self.session, **kwargs) as bt:
            throttled_requests = bt.multi_submit(reqs)

        return [r.response for r in throttled_requests]

    def mget(self, urls, **kw):
        log.debug('%s', urls)

        if isinstance(urls, basestring):
            urls = [urls]

        reqs = [requests.Request(method='GET', url=self.prepare_url(url), **kw)
                    for url in urls]

        return self.multi_submit(reqs)

    def get_paginated(self, path='', params={}, page_size=None):
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
        log.debug('%s, kwargs:%.512s <<<TRIMMED', url, data)
        if self.is_json(data):
            data = json_dumps(data)

        resp = self.session.post(url, data=data, **kw)
        if not resp.ok:
            return self.raise_or_log(resp)

        return resp

    def mpost(self, path='', dataset=[], **kw):
        url = self.prepare_url(path)
        log.debug('%s', url)
        reqs = [requests.Request(method='POST',
                                url=url,
                                data=json_dumps(data) if self.is_json(data) else data,
                                **kw) for data in dataset]
        return self.multi_submit(reqs)

    def post_paginated(self, path='', data={}, bulk_size=None, bulk_key=None):
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
        url = self.prepare_url(path)
        log.debug('%s, kwargs:%.512s <<<TRIMMED', url, data)

        if self.is_json(data):
            data = json_dumps(data)

        resp = self.session.put(url, data=data, **kw)
        if not resp.ok:
            return self.raise_or_log(resp)

        return resp

    def head(self, path='', params={}):
        resp = self.session.head(self.prepare_url(path, params))
        if not resp.ok:
            return self.raise_or_log(resp)

        return resp

    def delete(self, path='', **kw):
        url = self.prepare_url(path)
        log.debug(url)

        resp = self.session.delete(url, **kw)
        if not resp.ok:
            return self.raise_or_log(resp)

        return resp
