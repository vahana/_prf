import logging
from datetime import datetime
from bson import ObjectId, DBRef
import mongoengine as mongo

import prf.exc
from prf.utils import dictset, prep_params
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
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)

        return super(MongoJSONEncoder, self).default(obj)


class BaseMixin(object):

    '''Represents mixin class for models'''

    Q = mongo.Q

    @classmethod
    def get_collection(cls, **params):
        params, specials = prep_params(params)
        query_set = cls.objects

        try:
            query_set = query_set(**params)
            _total = query_set.count()
            if specials._count:
                return _total

            if specials._sort:
                query_set.order_by(*specials._sort)

            query_set = query_set[specials._offset:specials._offset + specials._limit]

            log.debug('get_collection.query_set: %s(%s)', cls.__name__,
                  query_set._query)
            query_set._total = _total
            
            return query_set

        except (mongo.ValidationError, mongo.InvalidQueryError) as e:
            raise prf.exc.HTTPBadRequest(str(e), extra={'data': e})

    @classmethod
    def get_resource(cls, **params):
        obj = cls.get_collection(**params).first()
        if not obj:
            raise prf.exc.HTTPNotFound("'%s(%s)' resource not found" % (cls.__name__, params))
        return obj

    @classmethod
    def get(cls, **kw):
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
        except mongo.queryset.MultipleObjectsReturned:
            raise prf.exc.HTTPBadRequest('Bad or Insufficient Params')

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        return cls.get_collection(id__in=ids, _limit=len(ids), **params)


class Base(BaseMixin, mongo.Document):

    # created_at = mongo.DateTimeField(default=datetime.utcnow)
    # updated_at = mongo.DateTimeField()

    meta = {'abstract': True}

    def raise_conflict(self, e):
        if e.__class__ is mongo.OperationError and 'E11000' \
                not in e.message:
            raise   # other error, not duplicate

        raise prf.exc.HTTPConflict(detail='Resource `%s` already exists.'
                 % self.__class__.__name__, extra={'data': e})

    def save(self, *arg, **kw):
        try:
            super(Base, self).save(*arg, **kw)
            return self
        except (mongo.NotUniqueError, mongo.OperationError), e:
            self.raise_conflict(e)
        except mongo.ValidationError, e:
            raise prf.exc.HTTPBadRequest("Resource '%s': %s"
                    % (self.__class__.__name__, e), extra={'data': e})

    def update(self, *arg, **kw):
        try:
            return super(Base, self).update(*arg, **kw)
        except (mongo.NotUniqueError, mongo.OperationError), e:
            self.raise_conflict(e)

    def validate(self, *arg, **kw):
        try:
            return super(Base, self).validate(*arg, **kw)
        except mongo.ValidationError, e:
            raise prf.exc.HTTPBadRequest("Resource '%s': %s"
                    % (self.__class__.__name__, e), extra={'data': e})
