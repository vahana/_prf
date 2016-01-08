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
EXCLUDED_FIELDS = ['-v', '-latest', '-id', '-self']


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


class Log(mongo.DynamicEmbeddedDocument):
    created_at = mongo.DateTimeField(default=datetime.utcnow)
    synced_at = mongo.DateTimeField(default=datetime.utcnow)
    updated_at = mongo.DateTimeField()
    tag = mongo.ListField(mongo.StringField())
    # importer = mongo.DictField()


class VersionedDocumentMetaclass(TopLevelDocumentMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(VersionedDocumentMetaclass, cls)
        attrs_meta = dictset(attrs.get('meta', {}))
        attrs_meta.setdefault('indexes', [])
        skip_versioning = False

        if attrs_meta.asbool('abstract', default=False):
            return super_new.__new__(cls, name, bases, attrs)

        attrs['v'] = mongo.IntField(default=1)
        attrs['latest'] = mongo.BooleanField(default=True)

        current_meta = get_document_meta(name)

        if current_meta:
            attrs_meta.update(current_meta)
        else:
            if 'uniques' in attrs_meta and attrs_meta['uniques']:
                attrs_meta['indexes'] += ['latest', 'v'] #why ?
                for each in attrs_meta.aslist('uniques', pop=True):
                    if each in attrs_meta['indexes']:
                        attrs_meta['indexes'].remove(each)

                    attrs_meta['indexes'] += [
                        {'fields': each + ['v'] if isinstance(each, list) else [each, 'v'],
                         'unique': True}
                    ]

        attrs['meta'] = attrs_meta
        new_class = super_new.__new__(cls, name, bases, attrs)
        new_class.set_collection_name()
        new_class.create_indexes()
        return new_class


class DatasetDoc(DynamicBase):
    __metaclass__ = VersionedDocumentMetaclass

    meta = {
        'abstract': True,
    }

    log = mongo.EmbeddedDocumentField(Log)
    ds_meta = mongo.DictField()

    @classmethod
    def set_collection_name(cls):
        cls._meta['collection'] = cls2collection(cls.__name__)

    @classmethod
    def _get_uniques(cls):
        return get_uniques(cls._meta['indexes'])

    @classmethod
    def get_ds_meta_params(cls, ds_meta):
        params = dictset()
        for key in cls._get_unique_meta_fields():
            if key not in ds_meta:
                continue
            params['ds_meta.%s'%key] = ds_meta[key]

        return params

    @classmethod
    def get_latest(cls, ds_meta):
        return cls.get(latest=True,
            **cls.get_ds_meta_params(ds_meta))

    @classmethod
    def _get_unique_meta_fields(cls):
        _fields = []
        for each in cls._get_uniques():
            for e in each:
                if e.startswith('ds_meta.'):
                    _fields.append(e[8:])

        return _fields

    def _unset_latest(self):
        cls = self.__class__
        params = cls.get_ds_meta_params(self.ds_meta)
        cls.objects(v__lt=self.v, **params)\
                   .update(set__latest=False)

    def clean(self):
        if self.log:
            if isinstance(self.log, dict):
                self.log = Log(**self.log)
        else:
            self.log = Log()

    def save_version(self, v=None, **kw):
        if v is None:
            latest = self.get_latest(self.ds_meta)
            v = latest.v + 1 if latest else 1

        self.v = v
        self.latest = True

        try:
            obj = super(DatasetDoc, self).save(**kw)
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
    def fix_verions(cls, **q):
        latest_objects = [dictset(each).extract(cls._get_uniques()+['max__as__v']) for each in
                            cls.get_collection(_group=cls._get_uniques(),
                                                _group_max='v',
                                                _group_list=cls._get_uniques(), **q)]

        cls.objects.update(set__latest=False)

        for each in latest_objects:
            cls.objects(**each).update(set__latest=True)



