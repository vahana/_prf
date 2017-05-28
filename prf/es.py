import logging
import urllib2
from bson import ObjectId, DBRef

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.serializer import JSONSerializer
from elasticsearch_dsl import Search, Q, A, DocType
from elasticsearch_dsl.connections import connections

import prf
from prf.utils import dictset, process_fields, split_strip, pager, chunks
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)

OPERATORS = ['ne', 'lt', 'lte', 'gt', 'gte', 'in',
             'startswith', 'exists', 'range']

PRECISION_THRESHOLD = 40000
DEFAULT_AGGS_LIMIT = 20
DEFAULT_AGGS_NESTED_LIMIT = 1000
TOP_HITS_MAX_SIZE = 100000
MAX_SKIP = 10000


def includeme(config):
    Settings = dictset(config.registry.settings)
    ES.setup(Settings)
    config.add_error_view(ElasticsearchException, error='%128s', error_attr='args')


class Base(DocType):
    pass

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

def prep_sort(specials, nested=None):
    sort = specials._sort
    nested = nested or {}
    new_sort = []

    for each in sort:
        if each.startswith('-'):
            order = 'desc'
            each = each[1:]
            missing = '_last'
        else:
            order = 'asc'
            missing = '_first'

        srt = {
                'order': order,
                'missing': missing,
        }

        if '_sort_mode' in specials:
            srt['mode'] = specials._sort_mode

        root = each.split('.')[0]
        if root in specials._nested:
            srt['nested_path'] = root
            if root in nested:
                srt['nested_filter'] = nested[root].to_dict()

        new_sort.append({each: srt})

    return new_sort

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

        field, _ = ES.dot_key(self.specials._distinct)

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
        _field = dictset()
        _field.params = dictset()

        _field.params['size'] = self.get_size()
        _field.bucket_name = field
        _field.field, _ = ES.dot_key(field)
        _field.op_type = 'terms'

        if '__as__' in field:
            field, _, _op = field.partition('__as__')
            if _op == 'geo':
                _field.op_type = 'geohash_grid'
                _field.params.precision = self.specials.asint('_geo_precision', default=5)
                _field.bucket_name = _field.field = field.replace(',', '_')

            elif _op == 'date_range':
                _field.bucket_name = field
                _field.op_type = 'date_range'
                _field.field = field
                _field.params.format = "MM-YY"
                _from, _to = self.specials.aslist('_ranges')
                _field.params.ranges = [{'from':_from}, {'to':_to}]
                _field.params.pop('size', None)

        return _field

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
    _meta_fields = ['_index', '_type', '_score', '_id']

    def __init__(self, data):
        self.data = dictset(data)
        self._meta = self.data.pop_many(self._meta_fields)

    def to_dict(self, fields=None):
        return self.data.extract(fields)

    def __getattr__(self, key):
        if key in self.data:
            return self.data[key]

        raise AttributeError()


class ES(object):

    @classmethod
    def dot_key(cls, key, suffix='keyword'):
        _key, div, op = key.rpartition('__')
        if div and op in OPERATORS:
            key = _key
        key = key.replace('__', '.')
        return ('%s.%s' % (key, suffix) if suffix else key), (op if op in OPERATORS else '')

    @classmethod
    def wrap_results(cls, specials, data, total, took):
        data = [ESDoc(each) for each in data]
        return wrap_results(specials, data, total, took)

    @classmethod
    def process_hits(cls, hits):
        data = []
        for each in hits:
            _d = dictset(each['_source'])
            _d = _d.update({
                '_score':each['_score'],
                '_type':each['_type'],
                '_index': each['_index'],
                '_id': each['_id'],
            })
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

    def __init__(self, name, ):
        self.index, _, self.doc_type = name.partition('/')

    def get_collection(self, **params):
        params = dictset(params)
        log.debug('(ES) IN: %s, params: %.1024s', self.index, params)

        _params, specials = prep_params(params)

        if specials._start > MAX_SKIP:
            raise prf.exc.HTTPBadRequest('Reached max pagination limit')

        s_ = Search(index=self.index, doc_type=self.doc_type)

        _ranges = []
        _filters = None

        _nested = {}
        specials.aslist('_nested', default=[])

        q_params = {'default_operator': 'and'}
        q_fields = specials.aslist('_q_fields', default=[], pop=True)
        if q_fields:
            q_params['fields'] = q_fields

        if '_q' in specials:
            q_params['query'] = specials._q
            s_ = s_.query('simple_query_string', **q_params)

        elif '_search' in specials:
            q_params['query'] = specials._search
            s_ = s_.query('query_string', **q_params)


        for key, val in _params.items():

            if isinstance(val, basestring) and ',' in val:
                val = _params.aslist(key)

            key, op = self.dot_key(key)
            root_key = key.split('.')[0]

            _filter = None

            if op in ['lt', 'lte', 'gt', 'gte']:
                _filter = Q('range', **{key: {op: val}})

            elif op in ['startswith']:
                if isinstance(val, list):
                    _filter = Q('bool',
                        should=[Q('prefix', **{key:each}) for each in val]
                    )
                else:
                    _filter = Q('prefix', **{key:val})

            elif op == 'exists':
                _filter = Q('exists', field=key)
                if val == 0:
                    _filter = ~_filter

            elif op == 'range':
                _range = []
                for _it in chunks(split_strip(val), 2):
                    rangeQ = Q('range', **{key: {'gte': _it[0]}})

                    if len(_it) == 2:
                        rangeQ = rangeQ & Q('range', **{key: {'lte': _it[1]}})

                    _range.append(rangeQ)

                _ranges.append(Q('bool', should=_range))

            elif val is None:
                _filter = Q('exists', field=key)
                if op != 'ne':
                    _filter = ~_filter

            elif isinstance(val, list):
                _filter = Q('bool',
                    should=[Q("term", **{key:each}) for each in val]
                )
                if op == 'ne':
                    _filter = ~_filter

            else:
                _filter = Q("term", **{key:val})
                if op == 'ne':
                    _filter = ~_filter

            if root_key in specials._nested:
                _nested[root_key] = _nested[root_key] & _filter if root_key in _nested else _filter
            else:
                _filters = _filters & _filter if _filters else _filter

        for path, nestedQ in _nested.items():
            q = Q('nested', path=path, query=nestedQ)
            _filters = _filters & q if _filters else q

        if _ranges:
            _filters = _filters & Q('bool', must=_ranges) if _filters else Q('bool', must=_ranges)

        if _filters:
            s_ = s_.filter(_filters)

        if specials._sort:
            s_ = s_.sort(*prep_sort(specials, _nested))

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
                return Aggregator(specials, s_, self.index).do_group()

            if specials._distinct:
                return Aggregator(specials, s_, self.index).do_distinct()

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
            # from pprint import pprint as pp;pp(s_.to_dict())
            log.debug('(ES) OUT: %s, query: %.2048s', self.index, s_.to_dict())

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
        results = self.get_collection(_limit=1, **params)
        try:
            return results['data'][0]
        except IndexError:
            raise prf.exc.HTTPNotFound("(ES) '%s(%s)' resource not found" % (self.index, params))

    def get(self, **params):
        results = self.get_collection(_limit=1, **params)
        if results['data']:
            return results['data'][0]
        else:
            return None

    def save(self, obj, data):
        data = dictset(data).unflat()
        return ES.api.update(
            index = obj._meta._index,
            doc_type = obj._meta._type,
            id = obj._meta._id,
            refresh=True,
            body = {'doc': data}
        )

    def delete(self, obj):
        return ES.api.delete(
            index = obj._meta._index,
            doc_type = obj._meta._type,
            id = obj._meta._id,
        )