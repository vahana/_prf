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

    import pyramid
    config.add_tween('prf.mongodb.mongodb_exc_tween',
                      under='pyramid.tweens.excview_tween_factory')


def mongodb_exc_tween(handler, registry):
    log.info('mongodb_tween enabled')

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
            raise prf.exc.HTTPBadRequest(request=request, exception=e)

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
    def get_collection(cls, **params):
        params, specials = prep_params(params)
        query_set = cls.objects

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

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        return cls.get_collection(id__in=ids, _limit=len(ids), **params)


class Base(BaseMixin, mongo.Document):
    meta = {'abstract': True}

    def update(self, *arg, **kw):
        for key in kw.copy():
            if not key.startswith('set__'):
                kw['set__%s' % key] = kw.pop(key)

        return super(Base, self).update(*arg, **kw)
