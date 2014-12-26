import logging
from datetime import datetime
from bson import ObjectId, DBRef
import mongoengine as mongo

from prf.json_httpexceptions import *
from prf.utils import process_fields, process_limit, split_strip, dictset, \
                      DataProxy, to_dicts, dict2obj
from prf.utils.dictset import asbool
from prf.view import BaseView
from prf.renderers import _JSONEncoder

log = logging.getLogger(__name__)


def includeme(config):
    Settings = dictset(config.registry.settings)

    db = Settings['mongodb.db']
    host = Settings.get('mongodb.host', 'localhost')
    port = Settings.asint('mongodb.port', 27017)

    log.info('MongoDB enabled with db:%s, host:%s, port:%s', db, host, port)

    mongo.connect(db=db, host=host, port=port)


class MongoJSONEncoder(_JSONEncoder):

    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)

        if isinstance(obj, DBRef):
            return str(obj)

        if hasattr(obj, 'to_dict'):
            # if it got to this point, it means its a nested object.
            # outter objects would have been handled with DataProxy.
            return obj.to_dict(__nested=True)
            return obj.to_dict()

        return super(MongoJSONEncoder, self).default(obj)


def process_lists(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'in' or _t == 'all':
            _dict[k] = _dict.aslist(k)
    return _dict


def process_bools(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'bool':
            _dict[new_k] = _dict.asbool(k, pop=True)

    return _dict


def apply_fields(query_set, _fields):
    fields_only, fields_exclude = process_fields(_fields)

    try:
        if fields_only:
            query_set = query_set.only(*fields_only)

        if fields_exclude:
            query_set = query_set.exclude(*fields_exclude)
    except mongo.InvalidQueryError, e:

        raise JHTTPBadRequest('Bad _fields param: %s ' % e)

    return query_set


def apply_sort(query_set, _sort):
    if not _sort:
        return query_set
    return query_set.order_by(*_sort)


class BaseMixin(object):

    '''Represents mixin class for models'''

    _auth_fields = None
    _public_fields = None
    _nested_fields = None

    _type = property(lambda self: self.__class__.__name__)
    Q = mongo.Q

    @classmethod
    def check_fields_allowed(cls, fields):
        if issubclass(cls, mongo.DynamicDocument):
            # dont check if its dynamic doc
            return

        fields = [f.split('__')[0] for f in fields]
        if not set(fields).issubset(set(cls.fields_to_query())):
            not_allowed = set(fields) - set(cls.fields_to_query())
            raise JHTTPBadRequest("'%s' object does not have fields: %s"
                                  % (cls.__name__, ', '.join(not_allowed)))

    @classmethod
    def filter_fields(cls, params):
        fields = cls.fields_to_query()
        for name in params:
            if name.split('__')[0] not in fields:
                params.pop(name)

        return params

    @classmethod
    def get_collection(cls, **params):
        """
        params may include '_limit', '_page', '_sort', '_fields'
        returns paginated and sorted query set
        raises JHTTPBadRequest for bad values in params
        """
        params = dictset(params)

        __confirmation = '__confirmation' in params
        params.pop('__confirmation', False)
        __strict = params.pop('__strict', True)

        _sort = split_strip(params.pop('_sort', []))
        _fields = split_strip(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)

        _count = '_count' in params
        params.pop('_count', None)
        _explain = '_explain' in params
        params.pop('_explain', None)
        __raise_on_empty = params.pop('__raise_on_empty', False)

        query_set = cls.objects

        if __strict:
            _check_fields = [f.strip('-+') for f in params.keys() + _sort]
            cls.check_fields_allowed(_check_fields)
        else:
            params = cls.filter_fields(params)

        process_lists(params)
        process_bools(params)

        # if param is _all then remove it
        params.pop_by_values('_all')

        try:
            query_set = query_set(**params)
            _total = query_set.count()
            if _count:
                return _total

            if _limit is None:
                raise JHTTPBadRequest('Missing _limit')

            _start, _limit = process_limit(_start, _page, _limit)

            # filtering by fields has to be the first thing to do on the
            # query_set!
            # query_set = apply_fields(query_set, _fields)

            query_set = apply_sort(query_set, _sort)
            query_set = query_set[_start:_start + _limit]

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if __raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)
        except (mongo.ValidationError, mongo.InvalidQueryError), e:

            raise JHTTPBadRequest(str(e), extra={'data': e})

        if _explain:
            return query_set.explain()

        log.debug('get_collection.query_set: %s(%s)', cls.__name__,
                  query_set._query)

        query_set._prf_meta = dict(total=_total, start=_start, fields=_fields)

        return query_set

    @classmethod
    def fields_to_query(cls):
        return [
            'id',
            '_limit',
            '_page',
            '_sort',
            '_fields',
            '_count',
            '_start',
            ] + cls._fields.keys()  # + cls._meta.get('indexes', [])

    @classmethod
    def get_resource(cls, **params):
        params.setdefault('__raise_on_empty', True)
        params['_limit'] = 1
        return cls.get_collection(**params).first()

    @classmethod
    def get(cls, **kw):
        return cls.get_resource(__raise_on_empty=kw.pop('__raise', False),
                                **kw)

    def unique_fields(self):
        return [e['fields'][0][0] for e in self._unique_with_indexes()] \
            + [self._meta['id_field']]

    def to_dict(self, request=None, **kw):
        _data = dictset([attr, getattr(self, attr, None)] for attr in
                        self._data)
        _data['_type'] = self._type
        _data.update(kw.pop('override', {}))
        return DataProxy(_data).to_dict(**kw)

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        try:
            return cls.objects.get(**params), False
        except mongo.queryset.DoesNotExist:
            defaults.update(params)
            return cls(**defaults).save(), True
        except mongo.queryset.MultipleObjectsReturned:
            raise JHTTPBadRequest('Bad or Insufficient Params')

    def _update(self, params, **kw):
        process_bools(params)
        for key, value in params.items():
            if key == self.__class__.id.name:  # cant change the primary key
                continue
            setattr(self, key, value)

        return self.save(**kw)

    def repr_parts(self):
        return []

    def __repr__(self):
        parts = []

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        if hasattr(self, '_version'):
            parts.append('v=%s' % self._version)

        parts.extend(self.repr_parts())

        return '<%s: %s>' % (self.__class__.__name__, ', '.join(parts))

    def update_iterables(self, params, attr, unique=False, value_type=None):
        is_dict = isinstance(type(self)._fields[attr], mongo.DictField)
        is_list = isinstance(type(self)._fields[attr], mongo.ListField)

        def split_keys(keys):
            neg_keys = []
            pos_keys = []

            for key in keys:
                if key.startswith('__'):
                    continue

                if key.startswith('-'):
                    neg_keys.append(key[1:])
                else:
                    pos_keys.append(key.strip())

            return pos_keys, neg_keys

        def update_dict():
            name = params.pop('name')

            if name.startswith('-'):
                self[attr].pop(name[1:], None)
            else:
                pos, neg = split_keys(params.keys())
                vals = pos + ['-%s' % n for n in neg]
                if not vals:
                    raise JHTTPBadRequest('missing params')

                if value_type == list:
                    self[attr][name] = vals
                else:
                    self[attr][name] = vals[0]

            self.save()

        def update_list():
            pos_keys, neg_keys = split_keys(params.keys())

            if not pos_keys + neg_keys:
                raise JHTTPBadRequest('missing params')

            if pos_keys:
                if unique:
                    self.update(**{'add_to_set__%s' % attr: pos_keys})
                else:
                    self.update(**{'push_all__%s' % attr: pos_keys})

            if neg_keys:
                self.update(**{'pull_all__%s' % attr: neg_keys})

        if is_dict:
            update_dict()
        elif is_list:

            update_list()

    @classmethod
    def get_by_ids(cls, ids, **params):
        return cls.get_collection(id__in=ids, _limit=len(ids), **params)

    @classmethod
    def expand_with(cls, with_cls, join_on=None, attr_name=None, params={},
                    with_params={}):
        """
            acts like "join" and inserts the with_cls objects in the result as "attr_name".
        """

        if join_on is None:
            join_on = with_cls.__name__.lower()
        if attr_name is None:
            attr_name = with_cls.__name__.lower()

        with_fields = with_params.pop('_fields', [])

        with_objs = with_cls.get_by_ids(cls.objects.scalar(join_on),
                **with_params)
        with_objs = dict([[str(wth.id), wth] for wth in with_objs])

        params['%s__in' % join_on] = with_objs.keys()
        objs = cls.get_collection(**params)

        for ob in objs:
            ob._data[attr_name] = with_objs[getattr(ob, join_on)]
            setattr(ob, attr_name, ob._data[attr_name])

        return objs


class BaseDocument(BaseMixin, mongo.Document):

    updated_at = mongo.DateTimeField()
    _version = mongo.IntField(default=0)

    meta = {'abstract': True}

    def save(self, *arg, **kw):
        if self._get_changed_fields():
            self.updated_at = datetime.utcnow()
            self._version += 1
        try:
            super(BaseDocument, self).save(*arg, **kw)
            return self
        except (mongo.NotUniqueError, mongo.OperationError), e:
            if e.__class__ is mongo.OperationError and 'E11000' \
                not in e.message:
                raise   # other error, not duplicate

            raise JHTTPConflict(detail='Resource `%s` already exists.'
                                % self.__class__.__name__, extra={'data': e})

    def update(self, *arg, **kw):
        try:
            return super(BaseDocument, self).update(*arg, **kw)
        except (mongo.NotUniqueError, mongo.OperationError), e:
            if e.__class__ is mongo.OperationError and 'E11000' \
                not in e.message:
                raise   # other error, not duplicate

            raise JHTTPConflict(detail='Resource `%s` already exists.'
                                % self.__class__.__name__, extra={'data': e})

    def validate(self, *arg, **kw):
        try:
            return super(BaseDocument, self).validate(*arg, **kw)
        except mongo.ValidationError, e:
            raise JHTTPBadRequest("Resource '%s': %s"
                                  % (self.__class__.__name__, e),
                                  extra={'data': e})


def get_document_cls(name):
    try:
        return mongo.document.get_document(name)
    except Exception, e:
        raise ValueError('`%s` does not exist in mongo db' % name)

class MongoView(BaseView):

    def __init__(self, context, request):
        super(MongoView, self).__init__(context, request)
        self._params.asint('_limit', default=20)

        def add_self(**kwargs):
            result = kwargs['result']
            request = kwargs['request']

            try:
                for each in result['data']:
                    each['self'] = '%s?id=%s' % (request.current_route_url(),
                            each['id'])
            except KeyError:
                pass

            return result

    def index(self):
        return 'Implement index action to return list of models'

    def show(self, id):
        cls = get_document_cls(id)
        return cls.get_collection(**self._params)

    def delete(self, id):
        cls = get_document_cls(id)
        objs = cls.get_collection(**self._params)

        if self.needs_confirmation():
            return objs

        count = len(objs)
        objs.delete()
        return JHTTPOk('Deleted %s %s objects' % (count, id))
