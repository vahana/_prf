import sys
import logging
from bson import ObjectId
import mongoengine as mongo
from datetime import datetime

import prf
from prf.mongodb import get_document_cls, DynamicBase,\
                        TopLevelDocumentMetaclass
from prf.utils import dictset, split_strip


log = logging.getLogger(__name__)
DS_COLL_PREFIX = 'ds_'


def cls2collection(name):
    return DS_COLL_PREFIX + name


def get_uniques(index_meta):
    for index in index_meta:
        if isinstance(index, dict) and index.get('unique', False):
            return [(each[1:] if each[0]=='-' else each)
                            for each in index['fields'] if each != '-v']

    return []


def get_document_meta(doc_name=None):
    db = mongo.connection.get_db()
    documen_metas = dictset()

    coll_names = db.collection_names()

    if doc_name and doc_name not in [e[len(DS_COLL_PREFIX):]
                                        for e in coll_names]:
        return dictset()

    for name in coll_names:
        if name.startswith(DS_COLL_PREFIX):
            _doc_name = name[3:]

            meta = dictset(
                _cls = _doc_name,
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

            if doc_name == _doc_name:
                return meta

            documen_metas[_doc_name] = meta

    return documen_metas


def define_document(name, meta={}, redefine=False):
    if not name:
        raise ValueError('Document class name can not be empty')

    name = str(name)

    meta['ordering'] = ['-id']

    if redefine:
        klass = type(name, (DatasetDoc,), {'meta': meta or {}})
    else:
        try:
            klass = get_document_cls(name)
        except ValueError:
            klass = type(name, (DatasetDoc,), {'meta': meta or {}})

    return klass


class Log(mongo.DynamicEmbeddedDocument):
    created_at = mongo.DateTimeField(default=datetime.utcnow)
    synced_at = mongo.DateTimeField(default=datetime.utcnow)
    updated_at = mongo.DateTimeField()
    tag = mongo.ListField(mongo.StringField())
    importer = mongo.DictField()


class VersionedDocumentMetaclass(TopLevelDocumentMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(VersionedDocumentMetaclass, cls)
        attrs_meta = dictset(attrs.get('meta', {}))
        attrs_meta.setdefault('indexes', [])

        if attrs_meta.asbool('abstract', default=False):
            return super_new.__new__(cls, name, bases, attrs)

        current_meta = get_document_meta(name)

        if current_meta:
            attrs_meta.update(current_meta)
        else:
            attrs_meta['indexes'] += ['latest', '-v']
            if 'unique' in attrs_meta and attrs_meta['unique']:
                uniques = attrs_meta.aslist('unique', pop=True)
                attrs_meta['indexes'] += [
                    {'fields': ['-v'] + uniques,
                     'unique': True}
                ]
                attrs_meta['indexes'] += uniques

        attrs['meta'] = attrs_meta
        attrs['v'] = mongo.IntField(default=1)
        attrs['latest'] = mongo.BooleanField(default=True)

        new_class = super_new.__new__(cls, name, bases, attrs)
        new_class.set_collection_name()
        new_class.create_indexes()
        return new_class


class DatasetDoc(DynamicBase):
    __metaclass__ = VersionedDocumentMetaclass

    COLL_PREFIX = 'ds_'
    meta = {'abstract': True}
    log = mongo.EmbeddedDocumentField(Log)

    @classmethod
    def set_collection_name(cls):
        cls._meta['collection'] = cls.COLL_PREFIX + cls.__name__

    @classmethod
    def _get_uniques(cls):
        return get_uniques(cls._meta['indexes'])

    def _unset_latest(self):
        params = {}
        cls = self.__class__
        for each in cls._get_uniques():
            params[each] = self[each]

        cls.objects(v__lt=self.v, **params)\
                   .update(set__latest=False)

    def clean(self):
        if not ObjectId.is_valid(self.id):
            self._foreign_id = self.id
            self.id = None

        if self.log:
            if isinstance(self.log, dict):
                self.log = Log(**self.log)
        else:
            self.log = Log()

    def get_last_version(self, key=None):
        params = {}
        cls = self.__class__

        if isinstance(key, basestring) and key:
            params[key] = self[key]
        else:
            uniques = cls._get_uniques()
            if not uniques:
                return None
            for each in uniques:
                if each not in self:
                    raise KeyError(
                        '`%s` unique key not found in %s' %
                                    (each, self.to_dict()))

                params[each] = self[each]

        obj = cls.objects(**params).order_by('-v').limit(1)
        if obj:
            return obj[0]

    def save(self, v=None, merge=False, skip_versioning=False, **kw):
        if skip_versioning:
            return super(DatasetDoc, self).save(**kw)

        try:
            obj = None
            if merge:
                obj = self.get_last_version(merge)
                if obj:
                    self.merge_with(obj.to_dict())

            if v:
                self.v = v
            else:
                if not merge:
                    obj = self.get_last_version()

                if obj:
                    self.v = obj.v + 1 # next version
                else:
                    self.v = 1

            self.latest = True
            obj = super(DatasetDoc, self).save(**kw)
            self._unset_latest()
            return obj

        except mongo.NotUniqueError as e:
            #try next version
            return self.save(v=self.v+1, merge=merge, **kw)
        except KeyError as e:
            log.error(e)

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


