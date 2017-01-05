import logging
import urllib2
from bson import ObjectId, DBRef

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.serializer import JSONSerializer
from elasticsearch_dsl import Search, Q, A
from elasticsearch_dsl.connections import connections

import prf
from prf.utils import dictset, process_fields, split_strip, pager
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)

OPERATORS = ['ne', 'lt', 'lte', 'gt', 'gte', 'in',
             'startswith', 'exists']

PRECISION_THRESHOLD = 40000
DEFAULT_AGGS_LIMIT = 20
DEFAULT_AGGS_NESTED_LIMIT = 1000
TOP_HITS_MAX_SIZE = 100000


def includeme(config):
    Settings = dictset(config.registry.settings)
    ES.setup(Settings)
    config.add_error_view(ElasticsearchException, error='%128s', error_attr='args')


class Serializer(JSONSerializer):
    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)

        return super(Serializer, self).default(obj)


def wrap_results(specials, data, total, took):
    return {
        'data': data,
        'total': total,
        'start': specials._start,
        'count': specials._limit,
        'fields': specials._fields,
        'sort': specials._sort,
        'took': took
    }


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

            return wrap_results(self.specials, data,
                                    resp.aggregations.total.value,
                                    resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    def check_total(self, msg):
        ss = Search.from_dict(self.search_obj.to_dict())
        ss._index = self.index
        resp = ss.execute()
        total = resp.aggregations.total.value
        if total > TOP_HITS_MAX_SIZE:
            raise prf.exc.HTTPBadRequest('`%s` results: %s' % (total, msg))

    def process_field(self, field):
        retval = dictset()
        retval.params = dictset()

        retval.params['size'] = self.get_size()
        retval.bucket_name = field
        retval.field = ES._raw_field(field)
        retval.op_type = 'terms'

        if '__as__' in field:
            field, _, _op = field.partition('__as__')
            if _op == 'geo':
                retval.op_type = 'geohash_grid'
                retval.params.precision = self.specials.asint('_geo_precision', default=5)
                retval.bucket_name = retval.field = field.replace(',', '_')

            elif _op == 'date_range':
                retval.bucket_name = field
                retval.op_type = 'date_range'
                retval.field = field
                retval.params.format = "MM-YY"
                _from, _to = self.specials.aslist('_ranges')
                retval.params.ranges = [{'from':_from}, {'to':_to}]
                retval.params.pop('size', None)

        return retval

    def build_agg_item(self, field_name, **params):
        field = self.process_field(field_name)
        field.params.update(params)

        if field.op_type in ['terms', 'geohash_grid']:
            return A(field.op_type,
                     field = field.field,
                     **field.params)
        elif field.op_type == 'date_range':
            return A('date_range',
                     field = field.field,
                     **field.params)

    def do_group(self):
        if '_show_hits' not in self.specials:
            self.search_obj = self.search_obj[0:0]

        top_field = self.process_field(self.specials._group[0])

        cardinality = A('cardinality',
                         field = top_field.field,
                         precision_threshold=PRECISION_THRESHOLD)

        self.search_obj.aggs.bucket('total', cardinality)

        if self.specials._count:
            resp = self.search_obj.execute()
            return resp.aggregations.total.value

        top_terms = self.build_agg_item(self.specials._group[0])
        if top_field.op_type == 'terms':
            top_terms._params['collect_mode']\
                 = self.specials.asstr('_collect_mode', default="breadth_first")

        aggs = top_terms
        for each in self.specials._group[1:]:
            field = self.process_field(each)
            field.params.size = DEFAULT_AGGS_NESTED_LIMIT
            aggs = aggs.bucket(field.bucket_name,
                    A(field.op_type,
                      field = field.field,
                      **field.params))

        self.search_obj.aggs.bucket(top_field.bucket_name, top_terms)

        if self.specials._count:
            return self.do_count(self.search_obj)

        try:
            resp = self.search_obj.execute()
            aggs = dictset(resp.aggregations._d_)
            hits = ES.process_hits(resp.hits.hits)

            data = dictset(
                    aggs = dictset(resp.aggregations._d_),
                    hits = hits
                )

            return wrap_results(self.specials,
                                    data,
                                    aggs.total.value,
                                    resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

        finally:
            log.debug('(ES) OUT: %s, query: %.512s', self.index, self.search_obj.to_dict())


class ESDoc(object):
    def __init__(self, data):
        self.data = dictset(data)

    def to_dict(self, fields):
        return self.data.extract(fields)

    def __getattr__(self, key):
        if key in self.data:
            return self.data[key]

        raise AttributeError()

class ES(object):
    RAW_FIELD = '.raw'

    @classmethod
    def _raw_field(cls, name):
        return '%s%s' % (name, cls.RAW_FIELD)

    @classmethod
    def wrap_results(cls, specials, data, total, took):
        data = [ESDoc(each) for each in data]
        return wrap_results(specials, data, total, took)

    @classmethod
    def process_hits(cls, hits):
        data = []
        for each in hits:
            _d = dictset(each['_source'])
            _d = _d.update({'_score':each['_score'], '_type':each['_type']})
            data.append(_d)

        return data

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
                                        timeout=cls.settings.asint('timeout', 30),
                                        serializer=Serializer(),
                                        **params)
            log.info('Including ElasticSearch. %s' % cls.settings)

        except KeyError as e:
            raise Exception('Bad or missing settings for elasticsearch. %s' % e)

    def __init__(self, name):
        self.name = name

    def get_collection(self, **params):
        params = dictset(params)
        log.debug('(ES) IN: %s, params: %.512s', self.name, params)

        _params, specials = prep_params(params)

        s_ = Search(index=self.name)
        _filter = None

        if '_q' in specials:
            q_fields = specials.aslist('_q_fields', default=[], pop=True)
            q_params = dict(
                query=specials._q,
                default_operator = "and"
            )

            if q_fields:
                q_params['fields'] = q_fields

            s_ = s_.query('simple_query_string', **q_params)

        elif '_search' in specials:
            s_ = s_.query('query_string', **{'query':specials._search})

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

            elif op == 'exists':
                existsQ = Q('exists', field=key)
                if val == 0:
                    existsQ = ~existsQ
                _filter = _filter & existsQ if _filter else existsQ
                continue

            if val is None:
                existsQ = Q('exists', field=key)
                if op != 'ne':
                    existsQ = ~existsQ
                _filter = _filter & existsQ if _filter else existsQ
                continue

            if isinstance(val, list):
                _orQ = Q('bool',
                    should=[Q("match", **{key:each}) for each in val]
                )
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

        try:
            if specials._count:
                return s_.count()

            if specials._group:
                return Aggregator(specials, s_, self.name).do_group()

            if specials._distinct:
                return Aggregator(specials, s_, self.name).do_distinct()

            if '_scan' in specials or specials._limit == -1:
                data = []
                for hit in s_.scan():
                    data.append(hit._d_)
                    if len(data) == specials._limit:
                        break

                return self.wrap_results(specials, data, s_.count(), 0)

            resp = s_.execute()
            data = self.process_hits(resp.hits.hits)
            return self.wrap_results(specials, data, resp.hits.total, resp.took)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

        finally:
            log.debug('(ES) OUT: %s, query: %.512s', self.name, s_.to_dict())

    def get_collection_paged(self, page_size, **params):
        params = dictset(params or {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))

        if _limit == -1:
            _limit = self.get_collection(_limit=_limit, _count=1, **params)

        log.debug('page_size=%s, _limit=%s', page_size, _limit)

        pgr = pager(_start, page_size, _limit)
        for start, count in pgr():
            _params = params.copy().update({'_start':start, '_limit': count})
            yield self.get_collection(**_params)['data']

    def get_resource(self, **params):
        results = self.get_collection(**params)
        try:
            return results['data'][0]
        except IndexError:
            raise prf.exc.HTTPNotFound("(ES) '%s(%s)' resource not found" % (self.name, params))

    def get(self, **params):
        results = self.get_collection(**params)
        if results['data']:
            return results['data'][0]
        else:
            return None

    # def delete(self, **params):
    #     if 'id' in params:
    #         self.api.delete(index=self.name, doc_type=self.name, id=id)
    #     else:
    #         self.api.delete_by_query(index=self.name, doc_type=self.name, q=params)


