import re
import sys
import logging
from types import ModuleType
import mongoengine as mongo
from datetime import datetime

import prf
from prf.mongodb import (
    DynamicBase, TopLevelDocumentMetaclass, Aggregator, BaseMixin
)
from prf.utils import dictset
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)
DS_COLL_PREFIX = ''
dataset_module_name = 'prf.dataset'


class DatasetStorageModule(ModuleType):
    pass


def cls2collection(name):
    return DS_COLL_PREFIX + name


def get_uniques(index_meta):
    uniques = []

    for index in index_meta:
        if isinstance(index, dict) and index.get('unique', False):
            uniques.append([
                (each[1:] if each[0] == '-' else each) for each in index['fields']
            ])

    return uniques


def get_dataset_names(match="", only_namespace=""):
    """
    Get dataset names, matching `match` pattern if supplied, restricted to `only_namespace` if supplied
    """
    namespaces = get_namespaces()
    names = []
    for namespace in namespaces:
        if only_namespace and only_namespace == namespace or not only_namespace:
            db = mongo.connection.get_db(namespace)
            for name in db.collection_names():
                if match in name.lower() and name.startswith(DS_COLL_PREFIX):
                    names.append([namespace, name, name[len(DS_COLL_PREFIX):]])
    return names


def get_namespaces():
    # Mongoengine stores connections as a dict {alias: connection}
    # Getting the keys is the list of aliases (or namespaces) we're connected to
    return mongo.connection._connections.keys()


def get_document_meta(alias, doc_name):
    db = mongo.connection.get_db(alias)

    name = cls2collection(doc_name)

    if name not in db.collection_names():
        return dictset()

    meta = dictset(
        _cls = doc_name,
        collection = name,
    )

    indexes = []
    for ix_name, index in db[name].index_information().items():
        fields = ['%s%s' % (('-' if order == -1 else ''), name)
                    for (name,order) in index['key']]

        indexes.append(dictset({'name': ix_name,
                        'fields':fields,
                        'unique': index.get('unique', False)}))

    meta['indexes'] = indexes

    return meta


def define_document(name, meta=None, namespace='default', redefine=False):
    if not name:
        raise ValueError('Document class name can not be empty')

    name = str(name)
    if not meta:
        meta = {}
    meta['ordering'] = ['-id']
    meta['db_alias'] = namespace

    if redefine:
        return type(name, (DatasetDoc,), {'meta': meta})

    try:
        return get_document(namespace, name)
    except AttributeError:
        return type(name, (DatasetDoc,), {'meta': meta})


def load_documents():
    names = get_dataset_names()
    for alias, _, _cls in names:
        doc = define_document(_cls)
        doc._meta['db_alias'] = alias
        log.info('Registering collection %s.%s', alias, _cls)
        set_document(alias, _cls, doc)


def safe_name(name):
    # Remove invalid characters
    cleaned = re.sub('[^0-9a-zA-Z_]', '', name)
    # Remove leading characters until we find a letter or underscore
    return re.sub('^[^a-zA-Z_]+', '', cleaned)


def namespace_storage_module(namespace, _set=False):
    namespace = safe_name(namespace)
    datasets_module = sys.modules[dataset_module_name]
    if _set:
        # If we're requesting to set and the target exists but isn't a dataset storage module
        # then we're reasonably sure we're doing something wrong
        if hasattr(datasets_module, namespace):
            if not isinstance(getattr(datasets_module, namespace), DatasetStorageModule):
                raise AttributeError('%s.%s already exists, not overriding.' % (dataset_module_name, namespace))
        else:
            setattr(datasets_module, namespace, DatasetStorageModule(namespace))
    return getattr(datasets_module, namespace, None)


def get_document(namespace, name, _raise=True):
    namespace_module = namespace_storage_module(namespace)
    cls_name = safe_name(name)
    if _raise:
        return getattr(namespace_module, cls_name)
    else:
        return getattr(namespace_module, cls_name, None)


def set_document(namespace, name, klass):
    namespace_module = namespace_storage_module(namespace, _set=True)
    setattr(namespace_module, safe_name(name), klass)


class Log(BaseMixin, mongo.DynamicEmbeddedDocument):
    created_at = mongo.DateTimeField(default=datetime.utcnow)
    updated_at = mongo.DateTimeField()
    tags = mongo.ListField(mongo.StringField())
    job = mongo.DictField()
    density = mongo.IntField()


class DSDocumentMetaclass(TopLevelDocumentMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(DSDocumentMetaclass, cls)
        attrs_meta = dictset(attrs.get('meta', {}))
        attrs_meta.setdefault('indexes', [])
        versioned = attrs_meta.pop('versioned', False)

        if attrs_meta.asbool('abstract', default=False):
            return super_new.__new__(cls, name, bases, attrs)

        pk_ = attrs_meta.aslist('pk', pop=True, default=[])

        current_meta = get_document_meta(attrs_meta.get('db_alias', 'default'), name)

        if current_meta:
            new_indexes = []
            for each in current_meta['indexes']:

                if each['name'] == '_id_': # skip this
                    continue

                new_indexes.append(each)
                if each['unique']:
                    if each['name'] == 'pk':
                        pk_ = [e[1:] if e[0]=='-' else e for e in each['fields']]
                        break

            current_meta['indexes'] = new_indexes
            attrs_meta.update(current_meta)

        elif versioned:
            attrs['v'] = mongo.IntField(default=1)
            attrs['latest'] = mongo.BooleanField(default=True)

            attrs_meta['indexes'].append('latest')
            attrs_meta['indexes'].append('v')

            if not pk_:
                raise prf.exc.HTTPBadRequest(
                    'must provide `target.pk` for versioned dataset `%s`'
                    ' or set `target.versioned=False`' % name)

            for each in pk_:
                attrs_meta['indexes'].append(each)

            pk_.append('v')
            attrs_meta['indexes'].append({
                'name': 'pk',
                'fields': pk_,
                'unique': True})
        else:
            for each in pk_:
                attrs_meta['indexes'].append({
                    'fields': pk_,
                    'unique': True
                })


        attrs['meta'] = attrs_meta
        new_class = super_new.__new__(cls, name, bases, attrs)
        new_class.set_collection_name()
        new_class.create_indexes()
        new_class._pk = pk_
        new_class._versioned = bool(pk_)
        return new_class


class DatasetDoc(DynamicBase):
    __metaclass__ = DSDocumentMetaclass

    meta = {
        'abstract': True,
    }

    log = mongo.EmbeddedDocumentField(Log)

    @classmethod
    def do_set(cls, params):
        params = dictset(params)
        format_error = '`_set` value must be in `<op>__<dataset_name>.<join_key>` format'
        params, specials = prep_params(params)

        _set = specials._set
        op, __, ds = _set.partition('__')
        other_query = {}

        if not op:
            raise prf.exc.HTTPBadRequest(format_error)

        if op not in ['in', 'nin']:
            raise prf.exc.HTTPBadRequest(
                    'operator must be `in` or `nin`')

        ds_name, _, join_key = ds.partition('.')
        if not join_key:
            raise prf.exc.HTTPBadRequest(format_error)

        for key,val in params.items():
            if key.startswith('%s.'%ds_name):
                other_query[key.partition('.')[2]] = params.pop(key)

        if op == 'in':
            params['%s__0__exists'%ds_name] = 1
        elif op == 'nin':
            params['%s__0__exists'%ds_name] = 0

        specials.update(dict(
            _join = '%s.%s' % (cls2collection(ds_name), join_key),
            _join_as = ds_name,
        ))

        aggr = Aggregator(cls.objects(**params)._query, specials)
        aggr._join_cond = other_query

        return aggr.join(cls._collection)


    @classmethod
    def set_collection_name(cls):
        cls._meta['collection'] = cls2collection(cls.__name__)

    @classmethod
    def _get_uniques(cls):
        return get_uniques(cls._meta['indexes'])

    def clean(self):
        if self.log:
            if isinstance(self.log, dict):
                self.log = Log(**self.log)
        else:
            self.log = Log()

        self.log.density = self.get_density()

    def get_params_from_pk(self):
        return self.to_dict(self._pk).subset('-v').flat()

    def get_latest(self):
        params = self.get_params_from_pk()
        if not params:
            log.warning('pk params empty for %s', params)
            return

        return self.get(latest=True, **params)

    def _unset_latest(self):
        cls = self.__class__
        params = self.get_params_from_pk()
        if not params:
            log.warning('pk params empty for %s', params)
            return

        cls.objects(v__lt=self.v, **params)\
                   .update(set__latest=False)

    def save_version(self, v=None, **kw):
        if v is None:
            latest = self.get_latest()
            self.v = latest.v + 1 if latest else 1
        else:
            self.v = v

        self.latest = True

        try:
            obj = super(DatasetDoc, self).save(**kw)
            if self.v > 1:
                self._unset_latest()
            return obj
        except mongo.NotUniqueError as e:
            return self.save_version(v=self.v+1, **kw)

    @classmethod
    def create_indexes(cls, name=None):
        try:
            if name is None:
                cls.ensure_indexes()
            else:
                cls.ensure_index(name)
        except Exception as e:
            raise prf.exc.HTTPBadRequest(str(e))

    @classmethod
    def drop_index(cls, name=None):
        try:
            if name is None:
                cls._collection.drop_indexes()
            else:
                cls._collection.drop_index(name)
        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)

    @classmethod
    def fix_versions(cls, **q):
        keys = [e for e in cls._pk if e != 'v']

        latest_objects = [dictset(each).extract(keys+['max__as__v']).flat() for each in
            cls.get_collection(
                _group=keys, _group_max='v',
                **q)]

        total = len(latest_objects)

        log.debug('Set latest to False for all')

        cls.objects(**q).update(set__latest=False)

        for each in latest_objects:
            log.debug('Processing %s: %s' % (total, each))
            total -=1

            each.pop_by_values(None)
            if each.keys() == ['v']:
                log.warning('WTF')
                continue

            cls.objects(**each).update(set__latest=True)

    @classmethod
    def get_collection(cls, _q=None, **params):
        if cls._versioned:
            params.setdefault('latest', True)
            if params['latest'] is None:
                params.pop('latest')

        return super(DatasetDoc, cls).get_collection(_q, **params)

