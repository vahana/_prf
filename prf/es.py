import logging
from six.moves.urllib.parse import urlparse
from pprint import pformat

from bson import ObjectId, DBRef

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.serializer import JSONSerializer
from elasticsearch_dsl import Search, Q, A, DocType
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import aggs as AGGS

from slovar import slovar
import prf
from prf.utils import (parse_specials, process_fields, split_strip,
                        pager, chunks, Params, process_key)
from prf.utils.errors import DValueError, DKeyError


log = logging.getLogger(__name__)

PRECISION_THRESHOLD = 40000
DEFAULT_AGGS_LIMIT = 20
DEFAULT_AGGS_NESTED_LIMIT = 1000
TOP_HITS_MAX_SIZE = 100000
MAX_RESULT_WINDOW = 10000


def includeme(config):
    Settings = slovar(config.registry.settings)
    ES.setup(Settings)
    config.add_error_view(ElasticsearchException, error='%128s', error_attr='args')


class Base(DocType):
    pass

class Serializer(JSONSerializer):
    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)

        return super(Serializer, self).default(obj)

def prep_sort(specials, nested=None):
    sort = specials._sort
    nested = nested or {}
    new_sort = []

    for each in sort:
        if each.startswith('-'):
            order = 'desc'
            each = each[1:]
            missing = specials.get('_sort_missing', '_last')
        else:
            order = 'asc'
            missing = specials.get('_sort_missing', '_first')

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


class ESDoc:
    def __init__(self, data, index, doc_types):
        self._data = slovar(data)
        self._index = index
        self._doc_types = doc_types

    def __repr__(self):
        parts = ['_index: %s' % self._index, '_id: %s' % self._data.get('_id', 'NA')]
        return '<%s>' % ', '.join(parts)

    def get(self, key):
        return self._data.get(key)

    def __getattr__(self, key):
        if key in self._data:
            return self._data[key]

        raise AttributeError(key)

    def __setattr__(self, key, val):
        if key in ['_data', '_index', '_doc_types']:
            super().__setattr__(key, val)
        else:
            self._data[key] = val

    def to_dict(self, fields=None):
        return self._data.extract(fields)


class Results(list):
    def __init__(self, index, specials, data, total, took):
        if not index:
            raise ValueError('index cant be None or empty')

        doc_types = ES.get_doc_types(index)
        list.__init__(self, [ESDoc(each, index=index, doc_types=doc_types) for each in data])
        self.total = total
        self.specials = specials
        self._meta = slovar(
            total = total,
            took = took,
            doc_types = doc_types,
            alias = ES.api.indices.get_alias(index)
        )


class Aggregator(object):

    def __init__(self, specials, search_obj, index):
        self.specials = specials
        self.specials.aslist('_group', default=[])
        self.metrics = []

        if self.specials._start or self.specials._page:
            raise prf.exc.HTTPBadRequest('_start/_page not supported in _group')

        for name,val in list(self.specials.items()):
            if name.startswith('_group_'):
                op = name[7:]
                self.metrics.append([op, val])

        self.search_obj = search_obj
        self.index=index

    @staticmethod
    def undot(name):
        return name.replace('.', '__')

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

        for (op, val) in self.metrics:
            bname = '%s_%s' % (self.undot(val), op)
            aggs.metric(bname, op, field=val)

        self.search_obj.aggs.bucket(top_field.bucket_name, top_terms)

        if self.specials._count:
            return self.do_count(self.search_obj)

        try:
            resp = self.search_obj.execute()
            aggs = slovar(resp.aggregations._d_)
            hits = ES.process_hits(resp.hits.hits)

            data = slovar(
                    aggs = slovar(resp.aggregations._d_),
                    hits = hits
                )

            return data

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

        finally:
            log.debug('(ES) OUT: %s, QUERY:\n%s', self.index, pformat(self.search_obj.to_dict()))

    def do_distinct(self):

        term_params = {
            'size': self.get_size(),
        }

        field, _ = process_key(self.specials._distinct)

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

            return data

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
        _field = slovar()
        _field.params = slovar()

        _field.params['size'] = self.get_size()
        _field.bucket_name = field
        _field.field, _ = process_key(field)
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


class ES(object):
    def __call__(self):
        return self

    @classmethod
    def process_hits(cls, hits):
        data = []
        for each in hits:
            _d = slovar(each['_source'])
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
                url = urlparse(each)
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
        self.index = name

    @classmethod
    def get_doc_types(cls, index):
        meta = cls.get_meta(index)
        if meta:
            for vv in meta.values():
                if not isinstance(vv, dict):
                    continue
                return list(vv.get('mappings', {}).keys())

    @classmethod
    def get_meta(cls, index, doc_type=None):
        return ES.api.indices.get_mapping(index, doc_type, ignore_unavailable=True)

    def drop_collection(self):
        ES.api.indices.delete(self.index, ignore=[400, 404])

    def unregister(self):
        pass

    def get_collection(self, **params):
        params = Params(params)
        log.debug('(ES) IN: %s, params: %s', self.index, pformat(params))

        _params, specials = parse_specials(params)

        def prefixedQ(key, val):
            if not isinstance(val, list):
                val = [val]

            items = []

            for each in val:
                op = 'term'
                if isinstance(each, str) and each.endswith('*'):
                    each = each.split('*')[0]
                    op = 'prefix'

                items.append(Q(op, **{key:each}))

            return items

        def check_pagination_limit():
            pagination_limit = self.settings.asint('max_result_window', default=MAX_RESULT_WINDOW)
            if specials._start > pagination_limit:
                raise prf.exc.HTTPBadRequest('Reached max pagination limit of `%s`' % pagination_limit)

        def get_exists(key):
            return Q('exists', field=key)

        s_ = Search(index=self.index)

        _ranges = []
        _filters = None

        _nested = {}
        specials.aslist('_nested', default=[])

        q_params = {'default_operator': 'and'}
        q_params['lowercase_expanded_terms'] = 'false'

        q_fields = specials.aslist('_q_fields', default=[], pop=True)
        if q_fields:
            q_params['fields'] = q_fields

        if '_q' in specials:
            q_params['query'] = specials._q
            s_ = s_.query('simple_query_string', **q_params)

        elif '_search' in specials:
            q_params['query'] = specials._search
            s_ = s_.query('query_string', **q_params)


        for key, val in list(_params.items()):
            list_has_null = False

            if isinstance(val, str) and ',' in val:
                val = _params.aslist(key)

                list_has_null = False
                if 'null' in val:
                    val.remove('null')
                    list_has_null=True
                elif val==[]:
                    list_has_null=True

            key, op = process_key(key)
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
                _filter = get_exists(key)
                if val == 0:
                    _filter = ~_filter

            elif op == 'range':
                _range = []
                for _it in chunks(val, 2):
                    rangeQ = Q('range', **{key: {'gte': _it[0]}})

                    if len(_it) == 2:
                        rangeQ = rangeQ & Q('range', **{key: {'lte': _it[1]}})

                    _range.append(rangeQ)

                if list_has_null:
                    _range.append(~get_exists(key))

                _ranges.append(Q('bool', should=_range))

            elif op == 'geobb':
                points = split_strip(val)
                if len(points) != 4:
                    raise prf.exc.HTTPBadRequest('geo bounding box requires 4 values: long,lat,long,lat. Got %s instead.' % points)

                _filter = Q('geo_bounding_box', **{
                        key: {
                            'top_left': ','.join(points[:2]),
                            'bottom_right': ','.join(points[-2:])
                        }
                    })

            elif val is None:
                _filter = get_exists(key)
                if op != 'ne':
                    _filter = ~_filter

            elif op == 'all':
                _filter = Q('bool', must=prefixedQ(key, val))

                if op == 'ne':
                    _filter = ~_filter

            elif isinstance(val, list) or op == 'in':
                _filter = Q('bool', should=prefixedQ(key, val))

                if list_has_null:
                    _filter |= ~get_exists(key)

                if op == 'ne':
                    _filter = ~_filter

            else:
                if isinstance(val, str):
                    _filter = prefixedQ(key, val)[0]
                else:
                    _filter = Q('term', **{key:val})

                if op == 'ne':
                    _filter = ~_filter

            if root_key in specials._nested:
                _nested[root_key] = _nested[root_key] & _filter if root_key in _nested else _filter

            elif _filter:
                _filters = _filters & _filter if _filters else _filter


        for path, nestedQ in list(_nested.items()):
            q = Q('nested', path=path, query=nestedQ)
            _filters = _filters & q if _filters else q

        if _ranges:
            _filters = _filters & Q('bool', must=_ranges) if _filters else Q('bool', must=_ranges)

        if _filters:
            s_ = s_.filter(_filters)

        if specials._count:
            return s_.count()

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

                return Results(self.index, specials, data, s_.count(), 0)

            check_pagination_limit()

            resp = s_.execute()
            data = self.process_hits(resp.hits.hits)
            return Results(self.index, specials, data, resp.hits.total, resp.took)

        finally:
            log.debug('(ES) OUT: %s, QUERY:\n%s', self.index, pformat(s_.to_dict()))

    def get_collection_paged(self, page_size, **params):
        params = slovar(params or {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))

        if _limit == -1:
            _limit = self.get_collection(_limit=_limit, _count=1, **params)

        log.debug('page_size=%s, _limit=%s', page_size, _limit)

        pgr = pager(_start, page_size, _limit)
        for start, count in pgr():
            _params = params.copy().update({'_start':start, '_limit': count})
            yield self.get_collection(**_params)

    def get_resource(self, **params):
        params['_limit'] = 1
        try:
            return self.get_collection(**params)[0].to_dict()
        except IndexError:
            raise prf.exc.HTTPNotFound("(ES) '%s(%s)' resource not found" % (self.index, params))

    def get(self, **params):
        return self.get_resource(**params)

    def get_total(self, **params):
        return self.get_resource(_count=1, **params)

    def save(self, obj, data):
        data = slovar(data).unflat()
        return ES.api.update(
            index = obj._meta._index,
            doc_type = obj._meta._type,
            id = obj._meta._id,
            refresh=True,
            detect_noop=True,
            body = {'doc': data}
        )

    def delete(self, obj):
        return ES.api.delete(
            index = obj._meta._index,
            doc_type = obj._meta._type,
            id = obj._meta._id,
        )
