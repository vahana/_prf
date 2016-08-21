import logging
import urllib2

from elasticsearch import Elasticsearch

import prf
from prf.utils import dictset, process_fields, split_strip
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)

def includeme(config):
    Settings = dictset(config.registry.settings)
    ES.setup(Settings)

class ES(object):

    @classmethod
    def setup(cls, settings):
        cls.settings = settings.unflat().es

        try:
            hosts = []
            for each in cls.settings.aslist('urls'):
                url = urllib2.urlparse.urlparse(each)
                hosts.append(dict(host=url.hostname, port=url.port))

            params = {}
            if cls.settings.asbool('sniff', default=False):
                params = dict(
                    sniff_on_start = True,
                    sniff_on_connection_fail = True
                )

            cls.api = Elasticsearch(hosts=hosts, *params)
            log.info('Including ElasticSearch. %s' % cls.settings)

        except KeyError as e:
            raise Exception('Bad or missing settings for elasticsearch. %s' % e)

    def __init__(self, name):
        self.name = name

    def get_collection(self, **params):
        from elasticsearch_dsl import Search, Q

        params = dictset(params)
        log.debug('IN: cls: (ES) %s, params: %.512s', self.name, params)

        s_ = Search(using=self.api, index=self.name)

        _params, specials = prep_params(params)
        _filter = None

        if 'q' in params:
            q_fields = _params.aslist('q_fields', default=[], pop=True)
            q_params = dict(
                query=_params.pop('q'),
                default_operator = "and"
            )

            if q_fields:
                q_params['fields'] = q_fields

            s_ = s_.query('simple_query_string', **q_params)

        for key, val in _params.items():
            negate = False

            if isinstance(val, basestring) and ',' in val:
                val = _params.aslist(key)

            keys = key.split('__')
            op = keys[-1]

            key, div, op = key.rpartition('__')
            key = key.replace(div, '.')

            if op == 'ne':
                key = '.'.join(keys[:-1])
                negate = True
            elif op == 'in':
                #the val is a list and will be handled as OR downstream
                pass
            elif op in ['lt', 'lte', 'gt', 'gte']:
                key = '.'.join(keys[:-1])
                rangeQ = Q('range', **{key: {op: val}})
                _filter = _filter & rangeQ if _filter else rangeQ
                continue
            else:
                key = '.'.join(keys)

            if val is None:
                missingQ = Q('missing', field=key)
                if negate:
                    missingQ = ~missingQ
                _filter = _filter & missingQ if _filter else missingQ
                continue

            if isinstance(val, list):
                _orQ = None
                for each in val:
                    _orQ = _orQ | Q("match", **{key:each}) if _orQ else Q("match", **{key:each})

                if _orQ:
                    if negate:
                        _orQ = ~_orQ
                    _filter = _filter & _orQ if _filter else _orQ

            else:
                _filter = _filter & Q("match", **{key:val}) if _filter else Q("match", **{key:val})
                if negate:
                    _filter = ~_filter

        if _filter:
            s_ = s_.query('bool', filter = _filter)

        if specials._sort:
            s_ = s_.sort(*specials._sort)

        if specials._end:
            s_ = s_[specials._start:specials._end]
        else:
            s_ = s_[specials._start:]

        if specials._fields:
            only, exclude = process_fields(specials._fields).mget(['only', 'exclude'])
            s_ = s_.source(include=['%s'%e for e in only],
                           exclude = ['%s'%e for e in exclude])


        try:
            resp = s_.execute()
            data = []

            for each in resp.hits.hits:
                _d = dictset(each['_source'])
                _d = _d.update({'_score':each['_score'], '_type':each['_type']})
                data.append(_d)

            return {
                'data': data,
                'total': resp.hits.total,
                'start': specials._start,
                'count': specials._limit,
                'fields': specials._fields,
                'took': resp.took
            }

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    def get_resource(self, **params):
        results = self.get_collection(**params)
        if results['total']:
            return dictset(results['data'][0])
        else:
            return dictset()