import logging
from six.moves.urllib.parse import urlparse
from pprint import pformat

from bson import ObjectId, DBRef

from elasticsearch.exceptions import ElasticsearchException, NotFoundError
from elasticsearch.serializer import JSONSerializer
from elasticsearch_dsl import Search, Q, A, DocType
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import aggs as AGGS
from elasticsearch_dsl.exceptions import UnknownDslObject

from elasticsearch import helpers

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
    config.add_tween('prf.es.es_exc_tween')


def es_exc_tween(handler, registry):
    log.info('mongodb_exc_tween enabled')

    def tween(request):
        try:
            return handler(request)

        except NotFoundError as e:
            raise prf.exc.HTTPNotFound(request=request, exception=e)
        except ElasticsearchException as e:
            raise prf.exc.HTTPBadRequest(request=request, exception=e)
        except UnknownDslObject as e:
            raise prf.exc.HTTPBadRequest(request=request, exception=e)


    return tween


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
            missing = specials.get('_sort_missing', '_last')

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
        self._data = data
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
        if fields:
            return slovar(self._data).extract(fields)
        return self._data

class Results(list):
    def __init__(self, index, specials, data, total, took, doc_types):
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

    METRICS_AGGS = [
        '_agg_avg', '_agg_sum', '_agg_max', '_agg_min',
        '_agg_stats', '_agg_percentiles', '_agg_cardinality' ]

    def __init__(self, specials, search_obj, index):
        self.specials = specials
        self.specials.aslist('_group', default=[])
        self.specials.aslist('_bucket_items', default=[])
        self.specials.asbool('_raw_', default=False)

        self.metrics = []

        if self.specials._start or self.specials._page:
            log.warning('_start/_page not supported in _group. Ignored.')
            self.specials.pop('_start', None)
            self.specials.pop('_page', None)

        for name,val in list(self.specials.items()):
            if name in self.METRICS_AGGS:
                op = name[5:]
                self.metrics.append([op, split_strip(val)])

        self.search_obj = search_obj
        self.index=index
        self.doc_types = ES.get_doc_types(index)

        if self.specials._group or self.metrics:
            cardinality = A('cardinality',
                             field = 'total',
                             precision_threshold=PRECISION_THRESHOLD)

            self.search_obj.aggs.bucket('total', cardinality)

    @classmethod
    def is_metrics(cls, specials):
        for name in specials:
            if name in cls.METRICS_AGGS:
                return True

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

    def transform(self, aggs):
        if self.specials._raw_:
            return aggs

        def _trans_metr(data):
            _d = slovar()

            def _clean(val):
                if 'value' in val:
                    return val['value']
                elif 'values' in val:
                    return val['values']
                else:
                    return val

            for kk, vals in self.metrics:
                for vv in vals:
                    metr_key = '%s_%s'%(vv, kk)
                    if metr_key in data:
                        _d[metr_key] = _clean(data[metr_key])

            return _d

        def _trans(_aggs, bucket_name, agg_names):
            '''recursive transformation
            Use _bucket_items to modify an item in the bucket.
            e.g. _group=address.country.name,address.admin1.name&_bucket_items=buckets.address.admin1.name__as__state
            '''

            buckets = []

            for bucket in _aggs[bucket_name]['buckets']:
                _d = slovar({bucket_name: bucket['key'], 'count': bucket['doc_count']})
                _d.update(_trans_metr(slovar(bucket)))

                for fld in self.specials._bucket_items:
                    parts = fld.split('buckets.')
                    if len(parts) == self.specials._group.index(bucket_name)+1:
                        _d = _d.extract('*,%s' % parts[-1])

                if not self.specials._flat:
                    _d = _d.unflat()

                if agg_names:
                    _d['buckets'] = _trans(bucket, agg_names[0], agg_names[1:])
                buckets.append(_d)

            return buckets

        total = 0

        if self.specials._group:
            bucket_name = self.specials._group[0]
            total = aggs[bucket_name]['sum_other_doc_count']
            data = _trans(aggs, bucket_name, self.specials._group[1:])
        elif self.metrics:
            data = [_trans_metr(aggs)]
        else:
            data = [aggs]

        total = total or len(data)

        if self.specials._count:
            return total

        return Results(self.index, self.specials, data, total, 0, doc_types=self.doc_types)

    def execute(self):
        try:
            resp = self.search_obj.execute()
            return self.transform(resp.aggregations._d_)

        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

        finally:
            log.debug('(ES) OUT: %s, QUERY:\n%s', self.index, pformat(self.search_obj.to_dict()))

    def do_group_range(self):
        val = [it.split(':') for it in split_strip(self.specials.get('_group_range'))]
        if not val:
            raise prf.exc.HTTPBadRequest('_group_range can not be empty')

        field = self.specials._group[0]
        ranges = []

        for it in val:
            item = {}
            if it[0]: item['from'] = it[0]
            if len(it) > 1 and it[1]: item['to'] = it[1]
            ranges.append(item)

        self.search_obj.aggs.bucket('%s_range' % field, 'range', field=field, ranges=ranges)\
                            .bucket('stats', 'stats')

        return self.execute()

    def do_metrics(self):
        for (op, val) in self.metrics:
            for each_val in val:
                self.search_obj.aggs.metric('%s_%s' % (self.undot(each_val), op), op, field=each_val)

        return self.execute()

    def do_group(self):
        if '_show_hits' not in self.specials:
            self.search_obj = self.search_obj[0:0]

        top_terms, top_field = self.build_agg_item(self.specials._group[0])

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
            for each in val:
                aggs.metric('%s_%s' % (self.undot(each), op), op, field=each)

        self.search_obj.aggs.bucket(top_field.bucket_name, top_terms)

        return self.execute()

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

        terms = A('terms', **term_params)
        self.search_obj.aggs.bucket('grouped', terms)

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
        field, _, _op = field.partition('__as__')

        _field = slovar()
        _field.params = slovar()

        _field.params['size'] = self.get_size()
        _field.field = field
        _field.bucket_name = '%s__%s' % (field, _op) if _op else field

        _field.op_type = 'terms'

        if _op:
            # make sure self.transform skips processing this
            self.specials._raw_ = True

            if _op == 'geo':
                _field.op_type = 'geohash_grid'
                _field.params.precision = self.specials.asint('_geo_precision', default=5)

            elif _op == 'date_range':
                _field.op_type = _op
                _field.params.format = self.specials.asstr('_format', default='yyyy-MM-dd')

                _from, _to = self.specials.aslist('_ranges')
                _field.params.ranges = [{'from':_from}, {'to':_to}]
                _field.params.pop('size', None)

            elif _op == 'date_histogram':
                _field.op_type = _op
                _field.params.interval = self.specials._interval
                _field.params.pop('size', None)
                _field.params.format = self.specials.asstr('_format', default='yyyy-MM-dd')

            else:
                _field.op_type = _op
                _field.params.pop('size')

        return _field

    def build_agg_item(self, field_name, **params):
        field = self.process_field(field_name)
        field.params.update(params)
        return A(field.op_type,
                 field = field.field,
                 **field.params), field


class ES(object):
    version = slovar(major=2, minor=4, patch=0)

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

            cls.version = cls._version()

            log.info('Including ElasticSearch. %s' % cls.settings)

        except KeyError as e:
            raise Exception('Bad or missing settings for elasticsearch. %s' % e)

    @classmethod
    def get_doc_types(cls, index):
        meta = cls.get_meta(index)
        if meta:
            for vv in meta.values():
                if not isinstance(vv, dict):
                    continue
                return list(vv.get('mappings', {}).keys())

    @classmethod
    def get_meta(cls, index, doc_type=None, command='get_mapping'):
        method = getattr(ES.api.indices, command)
        if cls.version.major >= 7:
            return method(index,
                ignore_unavailable=True)
        else:
            return method(index,
                doc_type,
                ignore_unavailable=True)

    @classmethod
    def get_alias_index_maps(cls, name):
        #name can be either alias or index
        aliases = slovar()
        indices = slovar()

        for index, alias in cls.get_meta(name, command='get_alias').items():
            allist = list(alias['aliases'].keys())
            indices.add_to_list(index, allist)
            for al in allist:
                aliases.add_to_list(al, index)

        return aliases, indices

    @classmethod
    def put_mapping(cls, **kw):
        if cls.version.major >= 7:
            kw.pop('doc_type', None)

        return ES.api.indices.put_mapping(**kw)


    @classmethod
    def _version(cls):
        try:
            vers = ES.api.info()['version']['number'].split('.')
        except Exception as e:
            return cls.version

        return slovar(major=int(vers[0]), minor=int(vers[1]), patch=int(vers[2]))

    @classmethod
    def flush(cls, data, args={}):
        args.setdefault('raise_on_error', False)
        args.setdefault('raise_on_exception', False)
        args.setdefault('refresh', True)

        success, all_errors = helpers.bulk(cls.api, data, **args)
        errors = []
        retries = []
        retry_data = []

        if all_errors:
            #separate retriable errors
            for err in all_errors:
                err = slovar(err)
                if err.fget('index.status') == 429: #too many requests
                    retries.append(err['index']['_id'])
                else:
                    errors.append(err)

                if retries:
                    for each in data:
                        if each['_id'] in retries:
                            retry_data.append(each)

        log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s',
                                len(data), success, len(errors), len(retry_data))
        return success, errors, retry_data


    def __init__(self, name):
        self.index = name
        self.name = name
        self.doc_types = ES.get_doc_types(name)
        self.alias_map, self.index_map = ES.get_alias_index_maps(name)

    def drop_collection(self):
        ES.api.indices.delete(self.index, ignore=[400, 404])

    def unregister(self):
        pass

    def build_search_object(self, params, specials, **extra):

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

        def get_exists(key):
            return Q('exists', field=key)

        _s = Search(index=self.index)

        _ranges = []
        _filters = None

        _nested = {}
        specials.aslist('_nested', default=[])

        q_params = self.settings.get('search', {})
        q_params.setdefault('default_operator', 'and')
        q_params.setdefault('lowercase_expanded_terms', 'false')

        q_fields = specials.aslist('_q_fields', default=[], pop=True)
        if q_fields:
            q_params['fields'] = q_fields

        if '_q' in specials:
            q_params['query'] = specials._q
            _s = _s.query('simple_query_string', **q_params)

        elif '_search' in specials:
            q_params['query'] = specials._search
            _s = _s.query('query_string', **q_params)

        for key, val in list(params.items()):
            list_has_null = False

            if isinstance(val, str) and ',' in val:
                val = params.aslist(key)

            if isinstance(val, list):
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
            _s = _s.filter(_filters)

        if specials._sort:
            _s = _s.sort(*prep_sort(specials, _nested))

        if ES.version.major > 2 and specials.get('_search_after'):
            _s = _s.extra(search_after=specials.aslist('_search_after'))

        if specials._end is not None:
            _s = _s[specials._start:specials._end]
        else:
            _s = _s[specials._start:]

        if specials._fields:
            op = process_fields(specials._fields)
            if not op.star:
                _s = _s.source(include=['%s'%e for e in op.only],
                               exclude = ['%s'%e for e in op.exclude])

        _s = _s.params(**extra)
        return _s

    def get_collection(self, **params):
        params = Params(params)
        log.debug('(ES) IN: %s, params: %s', self.index, pformat(params))

        _params, specials = parse_specials(params)

        def check_pagination_limit():
            pagination_limit = self.settings.asint('max_result_window', default=MAX_RESULT_WINDOW)
            if (specials._start or 0) > pagination_limit:
                raise prf.exc.HTTPBadRequest('Reached max pagination limit of `%s`' % pagination_limit)

        _s = self.build_search_object(_params, specials)

        try:
            if specials._group:
                return Aggregator(specials, _s, self.index).do_group()

            if specials.get('_group_range'):
                return Aggregator(specials, _s, self.index).do_group_range()

            if specials._distinct:
                return Aggregator(specials, _s, self.index).do_distinct()

            if Aggregator.is_metrics(specials):
                return Aggregator(specials, _s, self.index).do_metrics()

            if specials._count:
                return _s.count()

            check_pagination_limit()

            resp = _s.execute()
            data = self.process_hits(resp.hits.hits)
            return Results(self.index, specials, data, self.get_total(**params), resp.took,
                            doc_types=self.doc_types)

        finally:
            log.debug('(ES) OUT: %s, QUERY:\n%s', self.index, pformat(_s.to_dict()))

    def paginate(self, page_size, limit, params):
        _params, specials = parse_specials(params)

        _s = self.build_search_object(_params, specials, size=page_size, sort=['_doc'])
        log.debug('(ES) SCAN: %s, QUERY:\n%s', self.index, pformat(_s.to_dict()))

        data = []
        total = 0

        for hit in _s.scan():
            if total >= limit:
                break

            total += 1
            data.append(hit.to_dict())

            if len(data) >= page_size:
                yield Results(self.index, {}, data, 0, 0, self.doc_types)
                data = []

    def get_collection_paged(self, page_size, **params):
        params = Params(params or {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))

        if _limit == -1:
            _limit = self.get_total(**params)

        if params.asbool('_pagination', default=False, pop=True):
            for results in self.paginate(page_size, _limit, params):
                yield results
            return

        log.debug('page_size=%s, _limit=%s', page_size, _limit)
        pgr = pager(_start, page_size, _limit)
        results = []

        for start, count in pgr():
            params.update({'_start':start, '_limit': count})
            results = self.get_collection(**params)
            yield results

    def get_resource(self, **params):
        params['_limit'] = 1
        try:
            return self.get_collection(**params)[0].to_dict()
        except IndexError:
            raise prf.exc.HTTPNotFound("(ES) '%s(%s)' resource not found" % (self.index, params))

    def get(self, **params):
        params['_limit'] = 1
        try:
            return self.get_collection(**params)[0].to_dict()
        except IndexError:
            pass

    def get_total(self, **params):
        return self.get_collection(_count=1, **params)

    def save(self, obj, data):
        data = slovar(data).unflat()

        if self.version.major >= 7:
            return ES.api.update(
                index = obj._meta._index,
                id = obj._meta._id,
                refresh=True,
                detect_noop=True,
                body = {'doc': data}
            )
        else:
            return ES.api.update(
                index = obj._meta._index,
                doc_type = obj._meta._type,
                id = obj._meta._id,
                refresh=True,
                detect_noop=True,
                body = {'doc': data}
            )

    def delete(self, obj):
        if self.version.major >= 7:
            return ES.api.delete(
                index = obj._meta._index,
                id = obj._meta._id,
            )
        else:
            return ES.api.delete(
                index = obj._meta._index,
                doc_type = obj._meta._type,
                id = obj._meta._id,
            )


