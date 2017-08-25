import re
import sys
import logging
from types import ModuleType, ClassType
import mongoengine as mongo
from datetime import datetime

import prf
from prf.mongodb import (
    DynamicBase, TopLevelDocumentMetaclass, Aggregator, BaseMixin
)
from prf.utils import dictset
from prf.utils.qs import prep_params
from prf.utils.utils import maybe_dotted

log = logging.getLogger(__name__)
DS_COLL_PREFIX = ''
DATASET_MODULE_NAME = 'prf.dataset'
DATASET_NAMES_MAP = {}


class DatasetStorageModule(ModuleType):
    def __getattribute__(self, attr, *args, **kwargs):
        ns = ModuleType.__getattribute__(self, '__name__')
        cls = ModuleType.__getattribute__(self, attr, *args, **kwargs)
        if isinstance(cls, (type, ClassType)) and issubclass(cls, DynamicBase):
            cls._collection = None
            # Don't use .get(), we should crash of it doesn't exist
            cls._meta['db_alias'] = DATASET_NAMES_MAP[ns]
        return cls


def set_dataset_module(name):
    if not name:
        raise ValueError('Missing configuration option: dataset.module')
    # Just make sure the module gets loaded
    maybe_dotted(name)
    setattr(prf.dataset, 'DATASET_MODULE_NAME', name)


def connect_dataset_aliases(config):
    from prf.mongodb import mongo_connect

    ds = config.dataset_namespaces()
    if len(ds) == 1 and ds[0] == 'auto':
        ds = [str(x) for x in mongo.connection.get_connection().database_names()]
    for namespace in ds:
        connect_settings = config.prf_settings().update({
            'mongodb.alias': namespace,
            'mongodb.db': namespace
        })
        mongo_connect(connect_settings)

def dataset_namespaces(config):
    return (config.prf_settings().aslist('dataset.namespaces', '')
            or config.prf_settings().aslist('dataset.ns', ''))

def includeme(config):
    config.add_directive('dataset_namespaces', dataset_namespaces)
    set_dataset_module(config.prf_settings().get('dataset.module'))
    connect_dataset_aliases(config)


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


def get_dataset_names(match_name="", match_namespace=""):
    """
    Get dataset names, matching `match` pattern if supplied, restricted to `only_namespace` if supplied
    """
    namespaces = get_namespaces()
    names = []
    for namespace in namespaces:
        if match_namespace and match_namespace == namespace or not match_namespace:
            db = mongo.connection.get_db(namespace)
            for name in db.collection_names():
                if match_name in name.lower() and name.startswith(DS_COLL_PREFIX) and not name.startswith('system.'):
                    names.append([namespace, name, name[len(DS_COLL_PREFIX):]])
    return names


def get_namespaces():
    # Mongoengine stores connections as a dict {alias: connection}
    # Getting the keys is the list of aliases (or namespaces) we're connected to
    return mongo.connection._connections.keys()


def get_document_meta(namespace, doc_name):
    db = mongo.connection.get_db(namespace)

    name = cls2collection(doc_name)

    if name not in db.collection_names():
        return dictset()

    meta = dictset(
        _cls=doc_name,
        collection=name,
        db_alias=namespace,
    )

    indexes = []
    for ix_name, index in db[name].index_information().items():
        fields = [
            '%s%s' % (('-' if order == -1 else ''), name)
            for (name, order) in index['key']
        ]

        indexes.append(dictset({
            'name': ix_name,
            'fields':fields,
            'unique': index.get('unique', False)
        }))

    meta['indexes'] = indexes

    return meta


# TODO Check how this method is used and see if it can call set_document
def define_document(name, meta=None, namespace='default', redefine=False):
    if not name:
        raise ValueError('Document class name can not be empty')
    name = str(name)

    if '.' in name:
        namespace, _,name = name.partition('.')

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
    _namespaces = set()

    for namespace, _, _cls in names:
        # log.debug('Registering collection %s.%s', namespace, _cls)
        doc = define_document(_cls, namespace=namespace)
        set_document(namespace, _cls, doc)
        _namespaces.add(namespace)

    log.info('Loaded namespaces: %s', list(_namespaces))

def safe_name(name):
    # See https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    # Remove invalid characters
    cleaned = re.sub('[^0-9a-zA-Z_]', '', name)
    # Remove leading characters until we find a letter or underscore
    return re.sub('^[^a-zA-Z_]+', '', cleaned)


def namespace_storage_module(namespace, _set=False):
    safe_namespace = safe_name(namespace)
    datasets_module = sys.modules[DATASET_MODULE_NAME]
    if _set:
        # If we're requesting to set and the target exists but isn't a dataset storage module
        # then we're reasonably sure we're doing something wrong
        if hasattr(datasets_module, safe_namespace):
            if not isinstance(getattr(datasets_module, safe_namespace), DatasetStorageModule):
                raise AttributeError('%s.%s already exists, not overriding.' % (DATASET_MODULE_NAME, safe_namespace))
        else:
            DATASET_NAMES_MAP[safe_namespace] = namespace
            setattr(datasets_module, safe_namespace, DatasetStorageModule(safe_namespace))
    return getattr(datasets_module, safe_namespace, None)


def get_document(namespace, name, _raise=True):
    namespace_module = namespace_storage_module(namespace)
    cls_name = safe_name(name)
    if _raise:
        return getattr(namespace_module, cls_name)
    else:
        return getattr(namespace_module, cls_name, None)


def set_document(namespace, name, cls):
    namespace_module = namespace_storage_module(namespace, _set=True)
    setattr(namespace_module, safe_name(name), cls)


class Log(BaseMixin, mongo.DynamicEmbeddedDocument):
    created_at = mongo.DateTimeField(default=datetime.utcnow)
    updated_at = mongo.DateTimeField()
    tags = mongo.ListField(mongo.StringField())
    job = mongo.DictField()
    density = mongo.IntField()


class DatasetDoc(DynamicBase):
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

        return aggr.join(cls._get_collection())


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

    @classmethod
    def create_indexes(cls, name=None):
        try:
            if name is None:
                cls.ensure_indexes()
        except Exception as e:
            raise prf.exc.HTTPBadRequest(str(e))

    @classmethod
    def drop_index(cls, name=None):
        try:
            if name is None:
                cls._get_collection().drop_indexes()
            else:
                cls._get_collection().drop_index(name)
        except Exception as e:
            raise prf.exc.HTTPBadRequest(e)
