import logging
import urllib2

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A

import prf
from prf.utils import dictset, process_fields, split_strip
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)

OPERATORS = ['ne', 'lt', 'lte', 'gt', 'gte', 'in',
             'startswith']


def includeme(config):
    Settings = dictset(config.registry.settings)
    ES.setup(Settings)

class ES(object):
    RAW_FIELD = '.raw'

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

    def prefix_query(self, params, specials, _s):
        pass

    def aggregation(self, params, specials, s_):

        if specials._limit == -1:
            size = 0
        else:
            size = specials._limit or 20

        term_params = {
            'size': size,
        }

        if specials._group:
            field = '%s.raw'%specials._group
        elif specials._distinct:
            field = '%s.raw'%specials._distinct

            if specials._sort and specials._sort[0].startswith('-'):
                order = { "_term" : "desc" }
            else:
                order = {"_term": 'asc'}

            term_params['order'] = order

        term_params['field'] = field

        terms = A('terms', **term_params)
        cardinality = A('cardinality',
                         field = field,
                         precision_threshold=40000)

        s_.aggs.bucket('total', cardinality)

        if specials._count:
            resp = s_.execute()
            return resp.aggregations.total.value

        s_.aggs.bucket('list', terms)
        s_ = s_[0:0]

        try:
            resp = s_.execute()
            data = []

            if specials._group:
                for each in resp.aggregations.list.buckets:
                    datum = dictset({
                            specials._group:each.key,
                            'count': each.doc_count
                        }).unflat()
                    data.append(datum)

            elif specials._distinct:
                for each in resp.aggregations.list.buckets:
                    if specials._fields:
                        data.append({specials._fields[0]: each.key})
                    else:
                        data.append(each.key)

            return {
                'data': data,
                'total': resp.aggregations.total.value,
                'start': specials._start,
                'count': specials._limit,
                'fields': specials._fields,
                'took': resp.took
            }

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

        return {}

    def get_collection(self, **params):
        params = dictset(params)
        log.debug('IN: cls: (ES) %s, params: %.512s', self.name, params)

        _params, specials = prep_params(params)

        s_ = Search(using=self.api, index=self.name)
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

            if isinstance(val, basestring) and ',' in val:
                val = _params.aslist(key)

            _key, div, op = key.partition('__')
            if div and op in OPERATORS:
                key = _key.replace(div, '.')

            # match with non-analyzed version
            key = '%s%s'%(key, self.RAW_FIELD)

            if op in ['lt', 'lte', 'gt', 'gte']:
                rangeQ = Q('range', **{key: {op: val}})
                _filter = _filter & rangeQ if _filter else rangeQ
                continue

            elif op in ['startswith']:
                prefixQ = Q('prefix', **{key:val})
                _filter = _filter & prefixQ if _filter else prefixQ
                continue

            if val is None:
                missingQ = Q('missing', field=key)
                if op == 'ne':
                    missingQ = ~missingQ
                _filter = _filter & missingQ if _filter else missingQ
                continue

            if isinstance(val, list):
                _orQ = None
                for each in val:
                    _orQ = _orQ | Q("match", **{key:each}) \
                                if _orQ else Q("match", **{key:each})

                if _orQ:
                    if op == 'ne':
                        _orQ = ~_orQ
                    _filter = _filter & _orQ if _filter else _orQ

            else:
                matchQ = Q("match", **{key:val})
                if op == 'ne':
                    matchQ = ~matchQ

                _filter = _filter & matchQ if _filter else matchQ

        if _filter:
            s_ = s_.query('bool', filter = _filter)

        if specials._sort:
            s_ = s_.sort(*specials._sort)

        if specials._end is not None:
            s_ = s_[specials._start:specials._end]
        else:
            s_ = s_[specials._start:]

        if specials._fields:
            only, exclude = process_fields(specials._fields).mget(['only', 'exclude'])
            s_ = s_.source(include=['%s'%e for e in only],
                           exclude = ['%s'%e for e in exclude])

        if specials._group or specials._distinct:
            return self.aggregation(_params, specials, s_)

        if specials._count:
            return s_.count()

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