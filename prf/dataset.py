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
EXCLUDED_FIELDS = ['-v', '-latest', '-log', '-id', '-self']


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


def get_document_meta(doc_name=None):
    db = mongo.connection.get_db()
    documen_metas = dictset()

    names = get_dataset_names(doc_name or '')
    if not names:
        return dictset()

    for name, _doc_name in names:
        _doc_name = name[len(DS_COLL_PREFIX):]

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
    importer = mongo.DictField()


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
    def _get_unique_meta_fields(cls):
        _fields = []
        for each in cls._get_uniques():
            if each[0].startswith('ds_meta.'):
                _fields.append(each[0])

        return _fields

    def get_uniques_params(self):
        params = {}

        cls = self.__class__
        self_dict = self.to_dict().flat()
        unique_meta_fields = cls._get_unique_meta_fields()
        for each in unique_meta_fields:
            if each in self_dict:
                params[each] = self_dict[each]

        if not params:
            raise prf.exc.HTTPBadRequest('at least one of %s unique'
                                         ' fields must be present in %s'\
                                        % (unique_meta_fields, self_dict))

        return params

    def _unset_latest(self):
        cls = self.__class__
        params = self.get_uniques_params()
        cls.objects(v__lt=self.v, **params)\
                   .update(set__latest=False)

    def contains(self, other):
        other_dict = other.to_dict(EXCLUDED_FIELDS)
        return self.to_dict(other_dict.keys()) == other_dict

    def clean(self):
        if self.log:
            if isinstance(self.log, dict):
                self.log = Log(**self.log)
        else:
            self.log = Log()

    def get_latest_version(self, key=None):
        cls = self.__class__
        params = {}

        if isinstance(key, basestring) and key:
            params[key] = self[key]
        else:
            params = self.get_uniques_params()

        obj = cls.objects(**params).order_by('-v').limit(1)
        if obj:
            return obj[0]

    def save(self, v=None, merge=False, skip_versioning=False, **kw):
        if skip_versioning:
            return super(DatasetDoc, self).save(**kw)

        latest_obj = self.get_latest_version()
        if not latest_obj:
            self.v = 1
        else:
            if latest_obj.contains(self):
                return latest_obj # dont save if data did not change

            if merge:
                self.merge_with(latest_obj.to_dict(EXCLUDED_FIELDS))

            self.v = v or latest_obj.v + 1 # next version

        self.latest = True

        try:
            obj = super(DatasetDoc, self).save(**kw)
            self._unset_latest()
            return obj
        except mongo.NotUniqueError as e:
            return self.save(v=self.v+1, **kw)

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



