from __future__ import absolute_import
import logging
import elasticsearch
import mongoengine as mongo
from bson import ObjectId, DBRef
from prf.utils import dictset, dict2obj, process_limit, split_strip, to_dicts
from prf.json_httpexceptions import *

log = logging.getLogger(__name__)

RESERVED = [
    '_start',
    '_limit',
    '_page',
    '_fields',
    '_count',
    '_sort',
    '_raw_terms',
    ]


def on_pre_save(sender, document, **kw):
    if not kw.get('created', False) and document._get_changed_fields():
        ES(document.__class__.__name__).index(document.to_dict())


def on_post_save(sender, document, **kw):
    if kw.get('created', False):
        ES(document.__class__.__name__).index(document.to_dict())


def on_delete(sender, document, **kw):
    ES(document.__class__.__name__).delete(document.id)


def setup_es_signals_for(source_cls):
    mongo.signals.post_save.connect(on_post_save, sender=source_cls)
    mongo.signals.pre_save_post_validation.connect(on_pre_save,
            sender=source_cls)
    mongo.signals.post_delete.connect(on_delete, sender=source_cls)
    log.info('setup_es_signals_for: %r' % source_cls)


class ESHttpConnection(elasticsearch.Urllib3HttpConnection):

    def perform_request(self, *args, **kw):
        try:
            if log.level == logging.DEBUG:
                msg = str(args)
                if len(msg) > 512:
                    msg = msg[:300] + '...TRUNCATED...' + msg[-212:]
                log.debug(msg)

            return super(ESHttpConnection, self).perform_request(*args, **kw)
        except Exception, e:
            raise exception_response(400, detail='ElasticSearch Error',
                                     extra=dict(data=e))


class ESMetaclass(mongo.Document.my_metaclass):

    def __init__(self, name, bases, attrs):
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)


class MongoSerializer(elasticsearch.serializer.JSONSerializer):

    def default(self, data):
        if isinstance(data, (ObjectId, DBRef)):
            return str(data)
        try:
            return super(MongoSerializer, self).default(data)
        except:
            import traceback
            log.error(traceback.format_exc())


def includeme(config):
    settings = dictset(config.registry.settings)
    ES.setup(settings)


def apply_sort(_sort):
    _sort_param = []

    if _sort:
        for each in [e.strip() for e in _sort.split(',')]:
            if each.startswith('-'):
                _sort_param.append(each[1:] + ':desc')
            elif each.startswith('+'):
                _sort_param.append(each[1:] + ':asc')
            else:
                _sort_param.append(each + ':asc')

    return ','.join(_sort_param)


def build_terms(name, values, operator='OR'):
    return (' %s ' % operator).join(['%s:%s' % (name, v) for v in values])


def build_qs(params, _raw_terms='', operator='AND'):
    # if param is _all then remove it
    params.pop_by_values('_all')

    terms = []

    for k, v in params.items():
        if k.startswith('__'):
            continue
        if type(v) is list:
            terms.append(build_terms(k, v))
        else:
            terms.append('%s:%s' % (k, v))

    _terms = (' %s ' % operator).join(filter(bool, terms)) + _raw_terms

    return _terms


class _ESDocs(list):

    def __init__(self, *args, **kw):
        self._total = 0
        self._start = 0
        super(_ESDocs, self).__init__(*args, **kw)


class ES(object):

    api = None
    settings = None

    @classmethod
    def src2type(cls, source):
        return source.lower()

    @classmethod
    def setup(cls, settings):
        ES.settings = settings.mget('elasticsearch')

        try:
            _hosts = ES.settings.hosts
            hosts = []
            for host, port in [split_strip(each, ':') for each in
                               split_strip(_hosts)]:
                hosts.append(dict(host=host, port=port))

            params = {}
            if ES.settings.asbool('sniff', default=False):
                params = dict(sniff_on_start=True,
                              sniff_on_connection_fail=True)

            ES.api = elasticsearch.Elasticsearch(hosts=hosts,
                    serializer=MongoSerializer(),
                    connection_class=ESHttpConnection, **params)
            log.info('Including ElasticSearch. %s' % ES.settings)
        except KeyError, e:

            raise Exception('Bad or missing settings for elasticsearch. %s'
                            % e)

    def __init__(self, source='', index_name=None):
        self.doc_type = self.src2type(source)
        self.index_name = index_name or ES.settings.index_name

    def prep_bulk_documents(self, action, documents):
        if not (isinstance(documents, mongo.QuerySet) or isinstance(documents,
                list)):
            documents = [documents]

        _docs = []
        for doc in documents:
            if not isinstance(doc, dict):
                raise ValueError('document type must be `dict` not a %s'
                                 % type(doc))

            if '_type' in doc:
                _doc_type = self.src2type(doc['_type'])
            else:
                _doc_type = self.doc_type

            meta = {action: {
                'action': action,
                '_index': self.index_name,
                '_type': _doc_type,
                '_id': doc['id'],
                }}

            _docs.append([meta, doc])

        return _docs

    def _bulk(self, action, documents):
        if not documents:
            log.debug('empty documents: %s' % self.doc_type)
            return

        documents = self.prep_bulk_documents(action, documents)

        body = []
        for meta, doc in documents:
            action = meta.keys()[0]
            if action == 'delete':
                body += [meta]
            elif action == 'index':
                if 'timestamp' in doc:
                    meta['_timestamp'] = doc['timestamp']
                body += [meta, doc]

        if body:
            ES.api.bulk(body=body)
        else:
            log.warning('empty body')

    def index(self, documents):
        self._bulk('index', documents)

    def delete(self, ids):
        if not isinstance(ids, list):
            ids = [ids]

        self._bulk('delete', [{'id': _id, '_type': self.doc_type} for _id in
                   ids])

    def get_by_ids(self, ids, **params):
        if not ids:
            return _ESDocs()

        __raise_on_empty = params.pop('__raise_on_empty', False)
        fields = params.pop('_fields', [])

        _limit = params.pop('_limit', len(ids))
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)
        _start, _limit = process_limit(_start, _page, _limit)

        docs = []
        for _id in ids:
            docs.append(dict(_index=self.index_name,
                        _type=self.src2type(_id['_type']), _id=_id['_id']))

        params = dict(body=dict(docs=docs))
        if fields:
            params['fields'] = fields

        data = ES.api.mget(**params)
        documents = _ESDocs()

        for _d in data['docs']:
            try:
                _d = (_d['fields'] if fields else _d['_source'])
            except KeyError:
                msg = "ES: '%s(%s)' resource not found" % (_d['_type'],
                        _d['_id'])
                if __raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.error(msg)
                    continue

            documents.append(dict2obj(dictset(_d)))

        documents._prf_meta = dict(total=len(documents), start=_start,
                                   fields=fields)

        return documents

    def build_search_params(self, params):
        params = dictset(params)

        _params = dict(index=self.index_name, doc_type=self.doc_type)

        if 'body' not in params:
            query_string = build_qs(params.remove(RESERVED),
                                    params.get('_raw_terms', ''))
            if query_string:
                _params['body'] = \
                    {'query': {'query_string': {'query': query_string}}}
            else:

                _params['body'] = {'query': {'match_all': {}}}

        if '_limit' in params:
            _params['from_'], _params['size'] = \
                process_limit(params.get('_start', None), params.get('_page',
                              None), params['_limit'])

        if '_sort' in params:
            _params['sort'] = apply_sort(params['_sort'])

        if '_fields' in params:
            _params['fields'] = params['_fields']

        return _params

    def do_count(self, params):
        # params['fields'] = []
        params.pop('size', None)
        params.pop('from_', None)
        params.pop('sort', None)
        return ES.api.count(**params)['count']

    def get_collection(self, **params):
        __raise_on_empty = params.pop('__raise_on_empty', False)

        if 'body' in params:
            _params = params
        else:
            _params = self.build_search_params(params)

        if '_count' in params:
            return self.do_count(_params)

        # pop the fields before passing to search.
        # ES does not support passing names of nested structures
        _fields = _params.pop('fields', '')
        data = ES.api.search(**_params)
        documents = _ESDocs()

        for da in data['hits']['hits']:
            _d = (da['fields'] if 'fields' in _params else da['_source'])
            _d['_score'] = da['_score']
            documents.append(dict2obj(_d))

        documents._prf_meta = dict(total=data['hits']['total'],
                                   start=_params['from_'], fields=_fields,
                                   took=data['took'])

        if not documents:
            msg = "'%s(%s)' resource not found" % (self.doc_type, params)
            if __raise_on_empty:
                raise JHTTPNotFound(msg)
            else:
                log.debug(msg)

        return documents

    def get_resource(self, **kw):
        __raise = kw.pop('__raise_on_empty', True)

        params = dict(index=self.index_name, doc_type=self.doc_type)
        params.setdefault('ignore', 404)
        params.update(kw)

        data = ES.api.get_source(**params)
        if not data:
            msg = "'%s(%s)' resource not found" % (self.doc_type, params)
            if __raise:
                raise JHTTPNotFound(msg)
            else:
                log.debug(msg)

        return dict2obj(data)

    def get(self, **kw):
        kw['__raise_on_empty'] = kw.pop('__raise', False)
        return self.get_resource(**kw)

    @classmethod
    def index_refs(cls, mongo_obj):
        models = mongo_obj.__class__._meta['delete_rules'] or {}
        for model, key in models:
            if getattr(model, '_index_enabled', False):
                cls(model.__name__).index(to_dicts(model.objects(**{key: mongo_obj})))


from prf.mongodb import BaseDocument


class ESBaseDocument(BaseDocument):

    __metaclass__ = ESMetaclass

    meta = {'abstract': True}

    @classmethod
    def get(cls, **kw):
        return ES(cls.__name__).get(**kw)
