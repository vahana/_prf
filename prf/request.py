import logging
import requests
from urlparse import urljoin
from functools import partial

from prf.utils.utils import json_dumps, urlencode, pager
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
                      _raise=False,
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
            params = self.json(resp) or {'detail':resp.text}
            raise prf.exc.exception_response(status_code=resp.status_code,
                                    **params)

        log.error(str(self.json(resp)))
        return resp

    def from_cache(self, resp):
        return hasattr(resp, 'from_cache') and resp.from_cache

    def prepare_url(self, path='', params={}):
        if self.base_url:
            url = '%s/%s' % (self.base_url, path) if path else self.base_url
        else:
            url = path

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

        for req in throttled_requests:
            if not req.response.ok:
                self.raise_or_log(req.response)
            yield req.response

    def mget(self, urls=[], params=[], **kw):
        log.debug('%s', urls)

        reqs = []
        if isinstance(urls, basestring):
            urls = [urls]

        if urls:
            reqs = [requests.Request(method='GET',
                                     url=self.prepare_url(url), **kw)\
                    for url in urls]
        elif params:
            reqs = [requests.Request(method='GET',
                                     url=self.prepare_url('', param), **kw)\
                    for param in params]

        return self.multi_submit(reqs)

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

class PRFRequest(Request):
    def get_paginated(self, page_size, **kw):
        params = kw.get('params', {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))
        url_ptrn = '%s?_start=%%s&_limit=%%s' % self.base_url

        pagr = partial(pager, _start, page_size, _limit)

        if _limit == -1:
            for start, count in pagr():
                resp = self.get(url_ptrn % (start, count), **kw)
                if not resp.ok:
                    raise prf.exc._raise(resp)

                if resp.json()['count'] == 0:
                    break
                yield resp
        else:
            urls = [url_ptrn % (start, count) for (start, count) in pagr()]
            for resp in self.mget(urls, **kw):
                if resp.json()['count'] == 0:
                    break
                yield resp
