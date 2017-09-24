import os
import logging
import requests
from urlparse import urlparse, urljoin

from prf.utils.utils import json_dumps, urlencode, pager
from prf.utils import dictset

class DefaultExc(object):
    @classmethod
    def HTTPGatewayTimeout(cls, *args, **kw):
        raise ValueError(msg)
    @classmethod
    def exception_response(cls, *args, **kw):
        raise ValueError('%s'%kw)

try:
    import prf.exc as exc_kls
except ImportError:
    exc_kls = DefaultExc

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
            raise exc_kls.HTTPGatewayTimeout('%s for %s' % (str(e), e.request.url))


class Request(object):

    def __init__(self, base_url='', cache_options=None,
                      _raise=False,
                      delay=0, reqs_over_time = None,
                      cookies=None, headers=None):

        self.base_url = base_url.strip('/')
        cache_options = dictset(cache_options or {})
        self._raise = _raise
        self.delay = delay
        self.reqs_over_time = reqs_over_time or [] # [3,60] - 3 requests in 60 seconds

        if cache_options and cache_options.asbool('enable', default=False):
            import requests_cache
            log.debug('CachedSession for %s' % cache_options)
            cache_options.asstr('cache_name')
            cache_options.asfloat('expire_after')
            self.session = requests_cache.CachedSession(**cache_options)
            if cache_options.asbool('clear', default=False):
                self.session.cache.clear()
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

    def json(self, resp, err=''):
        try:
            json = resp.json()
            if isinstance(json, dict):
                return dictset(resp.json())
            else:
                return dictset(data=json)

        except Exception as e:
            log.error('Failed to convert to json: %s - %s' % (e, err))
            return dictset()

    def is_json(self, data):
        return isinstance(data, (tuple, list, dict)) \
            and self.session.headers['content-type'] == 'application/json'

    def raise_or_log(self, resp):
        if self._raise:
            params = self.json(resp) or {'detail':resp.text}
            raise exc_kls.exception_response(status_code=resp.status_code,
                                             **params)

        log.error(str(self.json(resp)))
        return resp

    def from_cache(self, resp):
        return hasattr(resp, 'from_cache') and resp.from_cache

    def prepare_url(self, path='', params={}, doseq=False):
        path = path.strip('/')
        path_ps = urlparse(path)
        url_ps = urlparse(self.base_url)

        if path:
            if not path_ps.netloc:
                url_ps = path_ps._replace(
                            scheme=url_ps.scheme,
                            netloc=url_ps.netloc,
                            path= '%s/%s' % (url_ps.path, path),
                    )
            else:
                url_ps = path_ps

        if params:
            new_query = urlencode(params, doseq)

            if path_ps.query:
                new_query = '%s&%s' % (new_query, path_ps.query)

            url_ps = url_ps._replace(query=new_query)

        return url_ps.geturl()

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

    def mget(self, urls=[], params=[], doseq=False, **kw):
        log.debug('%s', urls)

        reqs = []
        if isinstance(urls, basestring):
            urls = [urls]

        if urls:
            reqs = [requests.Request(method='GET',
                                     url=self.prepare_url(url, doseq=doseq),
                                     **kw) for url in urls]
        elif params:
            reqs = [requests.Request(method='GET',
                                     url=self.prepare_url(
                                        param.pop('path', ''), param,
                                        doseq=doseq), **kw
                            ) for param in params]

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

    def mpost(self, path='', payloads=[], **kw):
        url = self.prepare_url(path)
        log.debug('%s', url)
        reqs = [requests.Request(method='POST',
                                url=url,
                                data=json_dumps(data) if self.is_json(data) else data,
                                **kw) for data in payloads]
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

    def head(self, path='', params={}, **kw):
        resp = self.session.head(self.prepare_url(path, params), **kw)
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

    def download(self, path='', params={}, local_path='.',
                            local_name=None, chunk_size=4096, **kw):
        url = self.prepare_url(path, params)
        log.debug(url)

        local_name = local_name or urlparse(url).path.split('/')[-1]
        resp = self.get(url, stream=True, **kw)

        with open(os.path.join(local_path, local_name), 'wb') as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk: # filter out keep-alive new chunks
                    log.debug('writing %s chunk', len(chunk))
                    f.write(chunk)

        return resp


class PRFRequest(Request):
    def get_paginated(self, page_size, **kw):
        params = kw.pop('params', {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))

        pagr = pager(_start, page_size, _limit)

        if _limit == -1:
            for start, count in pagr():
                _params = params.copy().update({'_start':start, '_limit': count})
                resp = self.get(params=_params, **kw)
                if resp.ok and resp.json()['count'] == 0:
                    break
                yield resp
        else:
            _params =[]
            for start, count in pagr():
                _params.append({
                    '_start': start,
                    '_limit': count,
                })

            for resp in self.mget(params=_params, **kw):
                if resp.json()['count'] == 0:
                    break
                yield resp
