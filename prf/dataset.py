import sys
import logging
from bson import ObjectId
import mongoengine as mongo
from datetime import datetime

import prf
from prf.mongodb import (get_document_cls, DynamicBase,
                        TopLevelDocumentMetaclass, Aggregator,
                        BaseMixin)
from prf.utils import dictset, split_strip
from prf.utils.qs import prep_params

log = logging.getLogger(__name__)
DS_COLL_PREFIX = 'ds_'

def cls2collection(name):
    return DS_COLL_PREFIX + name


def get_uniques(index_meta):
    uniques = []

    for index in index_meta:
        if isinstance(index, dict) and index.get('unique', False):
            uniques.append( [(each[1:] if each[0]=='-' else each)
                            for each in index['fields']])

    return uniques


def get_dataset_names(match=""):
    db = mongo.connection.get_db()
    match = match.lower()
    return [[name, name[len(DS_COLL_PREFIX):]] for name in db.collection_names()
             if match in name.lower() and name.startswith(DS_COLL_PREFIX)]


def get_document_meta(doc_name):
    db = mongo.connection.get_db()

    name = DS_COLL_PREFIX + doc_name

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


def define_document(name, meta={}, redefine=False):
    if not name:
        raise ValueError('Document class name can not be empty')

    name = str(name)
    meta['ordering'] = ['-id']

    if redefine:
        return type(name, (DatasetDoc,), {'meta': meta})

    try:
        return get_document_cls(name)
    except ValueError:
        return type(name, (DatasetDoc,), {'meta': meta})


class Log(BaseMixin, mongo.DynamicEmbeddedDocument):
    created_at = mongo.DateTimeField(default=datetime.utcnow)
    updated_at = mongo.DateTimeField()
    tag = mongo.ListField(mongo.StringField())
    job = mongo.DictField()


class VersionedDocumentMetaclass(TopLevelDocumentMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(VersionedDocumentMetaclass, cls)
        attrs_meta = dictset(attrs.get('meta', {}))
        attrs_meta.setdefault('indexes', [])

        if attrs_meta.asbool('abstract', default=False):
            return super_new.__new__(cls, name, bases, attrs)

        attrs['v'] = mongo.IntField(default=1)
        attrs['latest'] = mongo.BooleanField(default=True)

        pk_ = []
        current_meta = get_document_meta(name)
        if current_meta:
            new_indexes = []
            for each in current_meta['indexes']:

                if each['name'] == '_id_': # skip this
                    continue

                new_indexes.append(each)
                if each['unique']:
                    pk_ = [e[1:] if e[0]=='-' else e for e in each['fields']]
                    if each['name'].startswith('pk_'):
                        break

            current_meta['indexes'] = new_indexes
            attrs_meta.update(current_meta)
        else:
            attrs_meta['indexes'].append('latest')
            attrs_meta['indexes'].append('v')

            pk_ = attrs_meta.aslist('pk', pop=True, default=[])

            for each in pk_:
                attrs_meta['indexes'].append(each)

            if pk_:
                pk_.append('v')
                attrs_meta['indexes'].append({
                    'name': 'pk_%s' % '_'.join(pk_),
                    'fields': pk_,
                    'unique': True})

        attrs['meta'] = attrs_meta
        new_class = super_new.__new__(cls, name, bases, attrs)
        new_class.set_collection_name()
        new_class.create_indexes()
        new_class._pk = pk_
        return new_class


class DatasetDoc(DynamicBase):
    __metaclass__ = VersionedDocumentMetaclass

    meta = {
        'abstract': True,
    }

    log = mongo.EmbeddedDocumentField(Log)
    ds_meta = mongo.DictField()

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

        # if self.ds_meta:
        #     if isinstance(self.ds_meta, dict):
        #         self.ds_meta = DSMeta(**self.ds_meta)
        # else:
        #     self.ds_meta = DSMeta()

    def get_params_from_pk(self):
        return self.to_dict(self._pk).subset('-v').flat()

    def get_latest(self):
        params = self.get_params_from_pk()
        if not params:
            log.warning('pk params empty for %s', self.ds_meta)
            return

        return self.get(latest=True, **params)

    def _unset_latest(self):
        cls = self.__class__
        params = self.get_params_from_pk()
        if not params:
            log.warning('pk params empty for %s', self.ds_meta)
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
            raise prf.exc.HTTPBadRequest(e)

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



