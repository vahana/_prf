import logging
from datetime import datetime
from bson import ObjectId, DBRef
import mongoengine as mongo

import prf.exc
from prf.utils import dictset, prep_params, split_strip, to_dunders, DValueError
from prf.renderers import _JSONEncoder

log = logging.getLogger(__name__)


def get_document_cls(name):
    try:
        return mongo.document.get_document(name)
    except Exception as e:
        raise DValueError('`%s` document does not exist' % name)


def drop_collections(name_prefix):
    db = mongo.connection.get_db()
    for name in db.collection_names():
        if name.startswith(name_prefix):
            log.warning('dropping `%s` collection' % name)
            db.drop_collection(name)


def includeme(config):
    mongo_connect(config.registry.settings)

    import pyramid
    config.add_tween('prf.mongodb.mongodb_exc_tween',
                      under='pyramid.tweens.excview_tween_factory')


Field2Default = {
    mongo.StringField : '',
    mongo.ListField : [],
    mongo.SortedListField : [],
    mongo.DictField : {},
    mongo.MapField : {},
}

def mongo_connect(settings):
    settings = dictset(settings)
    db = settings['mongodb.db']
    host = settings.get('mongodb.host', 'localhost')
    port = settings.asint('mongodb.port', 27017)

    log.info('MongoDB enabled with db:%s, host:%s, port:%s', db, host, port)

    mongo.connect(db=db, host=host, port=port)


def mongodb_exc_tween(handler, registry):
    log.info('mongodb_exc_tween enabled')

    def tween(request):
        try:
            return handler(request)

        except mongo.NotUniqueError as e:
            if 'E11000' in e.message:
                raise prf.exc.HTTPConflict(detail='Resource already exists.',
                            request=request, exception=e)
            else:
                raise prf.exc.HTTPBadRequest('Not Unique', request=request)

        except mongo.OperationError as e:
            raise prf.exc.HTTPBadRequest(e, request=request, exception=e)

        except mongo.ValidationError as e:
            raise prf.exc.HTTPBadRequest(e, request=request, exception=e)

        except mongo.InvalidQueryError as e:
            raise prf.exc.HTTPBadRequest(e, request=request, exception=e)

        except mongo.MultipleObjectsReturned:
            raise prf.exc.HTTPBadRequest('Bad or Insufficient Params',
                            request=request)

        except mongo.DoesNotExist as e:
            raise prf.exc.HTTPNotFound(request=request, exception=e)

    return tween


class MongoJSONEncoder(_JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)

        return super(MongoJSONEncoder, self).default(obj)


class BaseMixin(object):

    Q = mongo.Q

    @classmethod
    def process_empty_op(cls, name, value):
        _field = getattr(cls, name, None)
        try:
            _default = Field2Default[type(getattr(cls, name)) if _field else mongo.StringField]
        except KeyError:
            raise prf.exc.HTTPBadRequest(
                'Can not use `empty` for field `%s` of type %s'\
                    % (name, type(getattr(cls, name))))

        if int(value) == 0:
            return {'%s__ne' % name: _default}
        else:
            return {name: _default}

    @classmethod
    def prep_mongo_params(cls, params):
        list_ops = ('in', 'nin', 'all')
        for key in params:
            pos = key.rfind('__')
            if pos == -1:
                continue

            op = key[pos+2:]
            if op in list_ops:
                params[key] = split_strip(params[key])

            elif op == 'empty':
                params.update(cls.process_empty_op(key[:pos], params.pop(key)))

            elif op in ['exists', 'size', 'gt', 'gte', 'lt', 'lte']:
                params[key] = int(params[key])

        return params


    @classmethod
    def get_distinct(cls, queryset, specials):
        start = specials._offset
        end = specials._offset+specials._limit
        reverse = False

        if specials.asbool('_count', False):
            return len(queryset.distinct(specials._distinct))

        if specials._sort:
            if len(specials._sort) > 1:
                raise prf.exc.HTTPBadRequest('Must sort only on distinct')

            _sort = specials._sort[0]
            if _sort.startswith('-'):
                reverse = True
                _sort = _sort[1:]

            if _sort != specials._distinct:
                raise prf.exc.HTTPBadRequest('Must sort only on distinct')

        return sorted(queryset.distinct(specials._distinct), reverse=reverse)[start:end]

    @classmethod
    def get_group(cls, queryset, specials):
        aggr = []

        queryset = queryset or cls.objects

        def match(aggr):
            if queryset._query:
                aggr.append({'$match':queryset._query})
            return aggr

        def sort(aggr):
            sort_dict = {}
            for each in specials._sort:
                if each[0] == '-':
                    sort_dict[each[1:]] = -1
                else:
                    sort_dict[each] = 1
            if sort_dict:
                aggr.append({'$sort':sort_dict})

            return aggr

        def group(aggr):
            group_dict = {}
            for each in specials._group:
                group_dict[each] = '$%s' % each
            if group_dict:
                aggr.append({'$group': {'_id': group_dict}})

            return aggr


        def limit(aggr):
            start = specials._offset
            aggr.append({'$skip':start})

            if '_limit' in specials:
                end = specials._offset+specials._limit
                aggr.append({'$limit':end})
            return aggr


        def aggregate(aggr):
            log.debug(aggr)
            return [each['_id'] for each in cls._collection.aggregate(aggr, cursor={})]


        if specials.asbool('_count', False):
            return len(list(aggregate(group(match(aggr)))))

        return aggregate(limit(group(sort(match(aggr)))))

    @classmethod
    def group(cls, params, _limit=1):
        log.debug(params)
        specials = dictset(
            _group = params,
            _sort = [],
            _offset = 0,
        )
        if _limit:
            specials['_limit'] = _limit
        return cls.get_group(None, specials)


    @classmethod
    def get_collection(cls, **params):
        params = dictset(params)
        log.debug(params)
        params, specials = prep_params(params)
        params = cls.prep_mongo_params(params)

        start = specials._offset
        end = specials._offset+specials._limit

        query_set = cls.objects

        query_set = query_set(**params)
        _total = query_set.count()

        if specials._group:
            return cls.get_group(query_set, specials)

        if specials._distinct:
            return cls.get_distinct(query_set, specials)

        if specials.asbool('_count', False):
            return _total

        if specials._sort:
            query_set = query_set.order_by(*specials._sort)

        query_set = query_set[start:end]
        log.debug('get_collection.query_set: %s(%s)', cls.__name__, query_set._query)

        query_set._total = _total

        if specials._scalar:
            return query_set.scalar(*specials._scalar)

        return query_set

    @classmethod
    def get_resource(cls, **params):
        obj = cls.get_collection(**params).first()
        if not obj:
            raise prf.exc.HTTPNotFound("'%s(%s)' resource not found" % (cls.__name__, params))
        return obj

    @classmethod
    def get(cls, **params):
        return cls.get_collection(**params).first()

    def unique_fields(self):
        return [e['fields'][0][0] for e in self._unique_with_indexes()] \
            + [self._meta['id_field']]

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        try:
            return (cls.objects.get(**params), False)
        except mongo.queryset.DoesNotExist:
            defaults.update(params)
            return (cls(**defaults).save(), True)

    def repr_parts(self):
        return []

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        parts.extend(self.repr_parts())
        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        return cls.get_collection(id__in=ids, _limit=len(ids), **params)

    @property
    def id_str(self):
        return str(self.id)

    def update_with(self, _dict):
        for key, val in _dict.items():
            setattr(self, key, val)
        return self

    def merge_with(self, _dict):
        for attr, val in _dict.items():
            if not hasattr(self, attr):
                setattr(self, attr, val)

    def to_dict(self, fields=None):
        fields = fields or []
        _d = dictset(self.to_mongo().to_dict())

        if '_id' in _d:
            _d['id']=_d.pop('_id')

        if fields:
            _d = dictset(_d).subset(fields)

        return _d

    @classmethod
    def delete_if_exists(cls, **params):
        obj = cls.get(**params)
        if obj:
            obj.delete()

    @classmethod
    def _(cls, ix=0):
        return cls.objects[ix].to_dict()

    @classmethod
    def _count(cls):
        return cls.objects.count()

    @classmethod
    def _to_dict(cls, keyname, **params):
        return dictset([[e[keyname], ee]
                for e in cls.get_collection(**params)])


class Base(BaseMixin, mongo.Document):
    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(Base, self).update(*arg, **to_dunders(kw))
        return result


class DynamicBase(BaseMixin, mongo.DynamicDocument):
    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(DynamicBase, self).update(*arg, **to_dunders(kw))
        return result
