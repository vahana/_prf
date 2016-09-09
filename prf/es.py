import logging
import urllib2
from bson import ObjectId, DBRef

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.serializer import JSONSerializer
from elasticsearch_dsl import Search, Q, A
from elasticsearch_dsl.connections import connections

import prf
from prf.utils import dictset, process_fields, split_strip
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)

OPERATORS = ['ne', 'lt', 'lte', 'gt', 'gte', 'in',
             'startswith']

PRECISION_THRESHOLD = 40000
DEFAULT_AGGS_LIMIT = 20
TOP_HITS_MAX_SIZE = 1000

def includeme(config):
    Settings = dictset(config.registry.settings)
    ES.setup(Settings)
    config.add_error_view(ElasticsearchException, error='%128s', error_attr='args')

class Serializer(JSONSerializer):
    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)

        return super(Serializer, self).default(obj)


class Aggregator(object):

    def __init__(self, specials, search_obj, index):
        self.specials = specials
        self.specials.aslist('_group', default=[])
        self.specials.aslist('_group_list', default=[])

        self.search_obj = search_obj
        self.index=index

    def get_size(self):
        if self.specials._limit == -1:
            size = 0
        else:
            size = self.specials._limit or DEFAULT_AGGS_LIMIT

        return size

    def do_count(self):
        try:
            resp = self.search_obj.execute()
            return resp.aggregations.total.value
        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    def do_distinct(self):

        term_params = {
            'size': self.get_size(),
        }

        field = ES._raw_field(self.specials._distinct)

        if self.specials._sort and self.specials._sort[0].startswith('-'):
            order = { "_term" : "desc" }
        else:
            order = {"_term": 'asc'}

        term_params['order'] = order
        term_params['field'] = field

        cardinality = A('cardinality',
                         field = field,
                         precision_threshold=PRECISION_THRESHOLD)

        self.search_obj.aggs.bucket('total', cardinality)
        terms = A('terms', **term_params)
        self.search_obj.aggs.bucket('grouped', terms)

        if self.specials._count:
            return self.do_count(self.search_obj)

        try:
            resp = self.search_obj.execute()
            data = []
            for bucket in resp.aggregations.grouped.buckets:
                if self.specials._fields:
                    data.append({self.specials._fields[0]: bucket.key})
                else:
                    data.append(bucket.key)

            return ES.wrap_results(self.specials, data,
                                    resp.aggregations.total.value,
                                    resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    def do_group(self):

        top_field = self.specials._group[0]
        nested_fields = self.specials._group[1:]

        top_field_r = ES._raw_field(top_field)

        cardinality = A('cardinality',
                         field = top_field_r,
                         precision_threshold=PRECISION_THRESHOLD)

        self.search_obj.aggs.bucket('total', cardinality)

        if self.specials._group_list:
            #check the total to prevent top_hits running on big results
            ss = Search(index=self.index).from_dict(self.search_obj.to_dict())
            resp = ss.execute()
            total = resp.aggregations.total.value
            if total > TOP_HITS_MAX_SIZE:
                raise prf.exc.HTTPBadRequest('To many results for _group_list')

        if self.specials._count:
            resp = self.search_obj.execute()
            return resp.aggregations.total.value

        top_terms = A('terms', size=self.get_size(), field=top_field_r)

        # for field in nested_fields:
        #     top_terms.bucket(field,
        #             A('terms', size=0, field=ES._raw_field(field)))

        # if self.specials._group_list:
        #     top_hits = A('top_hits', _source={'include':self.specials._group_list},
        #                              size=TOP_HITS_MAX_SIZE)
        #     top_terms.bucket('list', top_hits)


        aggs = top_terms
        for field in nested_fields:
            aggs = aggs.bucket(field,
                    A('terms', size=0, field=ES._raw_field(field)))

        self.search_obj.aggs.bucket(top_field, top_terms)

        if self.specials._count:
            return self.do_count(self.search_obj)

        def process_buckets(parent_bucket, field):
            data = []

            for bucket in parent_bucket[field].buckets:
                _d = dictset({
                        field:bucket.key,
                        'count': bucket.doc_count,
                    })

                yield bucket, _d

        try:
            resp = self.search_obj.execute()
            data = []
            aggs = dictset(resp.aggregations._d_)
            return aggs

            data.append(aggs)

            for bucket, datum in process_buckets(aggs, top_field):

                # if self.specials._group_list:
                #     datum['list'] = [e['_source'] for e in bucket.list.hits.hits]

                for field in nested_fields:
                    datum[field] = [n_datum for _, n_datum
                                     in process_buckets(bucket, field)]

                data.append(datum)

            # for bucket in aggs.get(top_field).buckets:
            #     datum = dictset({
            #             top_field:bucket.key,
            #             'count': bucket.doc_count,
            #         })

            #     if self.specials._group_list:
            #         datum['list'] = [e['_source']._d_ for e in bucket.list.hits.hits]

            #     data.append(datum.unflat())

            return ES.wrap_results(self.specials, data,
                                    aggs.total.value,
                                    resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)


class ES(object):
    RAW_FIELD = '.raw'

    @classmethod
    def _raw_field(cls, name):
        return '%s%s' % (name, cls.RAW_FIELD)

    @classmethod
    def wrap_results(cls, specials, data, total, took):
        return {
            'data': data,
            'total': total,
            'start': specials._start,
            'count': specials._limit,
            'fields': specials._fields,
            'took': took
        }

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

            cls.api = connections.create_connection(hosts=hosts,
                                                timeout=20,
                                                serializer=Serializer(),
                                                **params)
            log.info('Including ElasticSearch. %s' % cls.settings)

        except KeyError as e:
            raise Exception('Bad or missing settings for elasticsearch. %s' % e)

    def __init__(self, name):
        self.name = name

    def get_collection(self, **params):
        params = dictset(params)
        log.debug('IN: cls: (ES) %s, params: %.512s', self.name, params)

        _params, specials = prep_params(params)

        s_ = Search(index=self.name)
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

            _key, div, op = key.rpartition('__')
            if div and op in OPERATORS:
                key = _key.replace(div, '.')

            # match with non-analyzed version
            key = self._raw_field(key)

            if op in ['lt', 'lte', 'gt', 'gte']:
                rangeQ = Q('range', **{key: {op: val}})
                _filter = _filter & rangeQ if _filter else rangeQ
                continue

            elif op in ['startswith']:
                prefixQ = Q('prefix', **{key:val})
                _filter = _filter & prefixQ if _filter else prefixQ
                continue

            if val is None:
                existsQ = Q('exists', field=key)
                if op != 'ne':
                    existsQ = ~existsQ
                _filter = _filter & existsQ if _filter else existsQ
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

        _fields = specials._fields
        if _fields:
            only, exclude = process_fields(_fields).mget(['only', 'exclude'])
            s_ = s_.source(include=['%s'%e for e in only],
                           exclude = ['%s'%e for e in exclude])

        if specials._group:
            return Aggregator(specials, s_, self.name).do_group()

        elif specials._distinct:
            return Aggregator(specials, s_, self.name).do_distinct()

        if specials._count:
            return s_.count()

        try:
            resp = s_.execute()
            data = []

            for each in resp.hits.hits:
                _d = dictset(each['_source'])
                _d = _d.update({'_score':each['_score'], '_type':each['_type']})
                data.append(_d)

            return self.wrap_results(specials, data, resp.hits.total, resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    def get_resource(self, **params):
        results = self.get_collection(**params)
        if results['total']:
            return dictset(results['data'][0])
        else:
            return dictset()
