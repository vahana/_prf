import logging
from datetime import datetime
from bson import ObjectId, DBRef
import mongoengine as mongo
from mongoengine.base import TopLevelDocumentMetaclass as TLDMetaclass
import pymongo

import prf.exc
from prf.utils import dictset, split_strip,\
                      to_dunders, process_fields, qs2dict
from prf.utils.qs import prep_params
from prf.renderers import _JSONEncoder

log = logging.getLogger(__name__)


class TopLevelDocumentMetaclass(TLDMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(TopLevelDocumentMetaclass, cls)
        attrs_meta = dictset(attrs.get('meta', {}))

        new_klass = super_new.__new__(cls, name, bases, attrs)

        if attrs_meta.pop('enable_signals', False):
            for signal in mongo.signals.__all__:
                if hasattr(new_klass, signal):
                    method = getattr(new_klass, signal)
                    if callable(method):
                        getattr(mongo.signals, signal).connect(method, sender=new_klass)

        return new_klass

def get_document_cls(name, _raise=True):
    try:
        return mongo.document.get_document(name)
    except Exception as e:
        if _raise:
            raise dictset.DValueError('`%s` document does not exist' % name)


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
    port = settings.asint('mongodb.port', default=27017)

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

        except (mongo.OperationError,
                mongo.ValidationError,
                mongo.InvalidQueryError,
                pymongo.errors.OperationFailure) as e:
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
        try:
            _field = getattr(cls, name)
            _default = Field2Default[type(_field)]
        except (KeyError, AttributeError) as e:
            raise prf.exc.HTTPBadRequest(
                'Can not use `empty` for field `%s`: dynamic field or unknown type' % (name, ))

        if int(value) == 0:
            return {'%s__ne' % name: _default}
        else:
            return {name: _default}


    @classmethod
    def get_frequencies(cls, queryset, specials):
        specials.asstr('_frequencies',  allow_missing=True)
        specials.asbool('_fq_normalize',  default=False)

        reverse = not bool(specials._sort and specials._sort[0].startswith('-'))
        for each in  sorted(
            queryset.item_frequencies(specials._frequencies,
            normalize=specials.asbool('_fq_normalize', default=False)
        ).items(),
        key=lambda x:x[1],
        reverse=reverse)[specials._start:specials._limit]:
            yield({each[0]:each[1]})

    @classmethod
    def get_distinct(cls, queryset, specials):
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

        dset = sorted(queryset.distinct(specials._distinct), reverse=reverse)

        if specials._end is None:
            return dset[specials._start:]
        else:
            return dset[specials._start: specials._end]

    @classmethod
    def get_group(cls, queryset, specials):
        aggr = []
        specials.aslist('_group', allow_missing=True)
        match_query = queryset._query

        accumulators = dictset([[e[7:],specials[e]] \
            for e in specials if e.startswith('_group_')])

        def undot(name):
            return name.replace('.', '__')

        def match(aggr):
            if match_query:
                aggr.append({'$match':match_query})
            return aggr

        def unwind(aggr):
            _prj = {"_id": "$_id"}
            unwinds = []

            for op, name in accumulators.copy().items():
                if op != 'unwind':
                    continue

                accumulators.pop(op)
                num_dots = name.count('.')
                if num_dots:
                    new_name = undot(name)
                    # accumulators[op] = new_name
                    _prj[new_name]='$%s' % name
                    for x in range(num_dots):
                        unwinds.append({'$unwind': '$%s' % new_name})
                else:
                    _prj[name]='$%s' % name
                    unwinds.append({'$unwind': '$%s' % name})

            if unwinds:
                aggr.append({'$project':_prj})
                aggr.extend(unwinds)

            return aggr

        def group(aggr):
            group_dict = {}

            for each in specials._group:
                group_dict[undot(each)] = '$%s' % each

            if group_dict:
                _d = {'_id': group_dict,
                      'count': {'$sum':1}}

                for op, val in accumulators.items():
                    _op = op.lower()
                    if _op in ['addtoset', 'set']:
                        sfx = 'set'
                        op = '$addToSet'
                    elif _op in ['push', 'list']:
                        sfx = 'list'
                        op = '$push'
                    else:
                        sfx = op
                        op = '$%s'%sfx


                    if val == '$ROOT':
                        _d[sfx] = {op:'$$ROOT'}
                        continue

                    _dd = {}

                    if sfx in ['set', 'list']:
                        for _v in split_strip(val):
                            _dd[undot(_v)] = '$%s' % _v
                    else:
                        _dd = '$%s' % val

                    _d[sfx] = {op:_dd}

                aggr.append({'$group':_d})

            return aggr

        def project(aggr):
            _prj = {'_id':0, 'count':1}
            for each in specials._group:
                _prj[each] = '$_id.%s' % undot(each)

            _gkeys = {}
            for each in aggr:
                if '$group' in each:
                    _gkeys = each['$group'].keys()

            for each in _gkeys:
                if each == '_id':
                    continue
                for _v in split_strip(each):
                    _prj[_v] = '$%s' % undot(_v)

            aggr.append(
                {'$project': _prj}
            )
            return aggr

        def sort(aggr):
            sort_dict = {}

            for each in specials._sort:
                if each[0] == '-':
                    sort_dict[each[1:]] = -1
                else:
                    sort_dict[each] = 1

            sort_dict = sort_dict or {'count': -1}
            aggr.append({'$sort':sort_dict})

            return aggr

        def limit(aggr):
            aggr.append({'$skip':specials._start})
            if specials._end is not None:
                aggr.append({'$limit':specials._end})

            return aggr

        def aggregate(aggr):
            log.debug(aggr)
            return cls._collection.aggregate(aggr, cursor={},
                                             allowDiskUse=True)

        if specials.asbool('_count', False):
            aggr = group(match(aggr))
            return len(list(aggregate(aggr)))

        aggr = limit(sort(project(group(unwind(match(aggr))))))

        if specials.asbool('_asdict', False):
            return dict([
                         [e[specials._group[0]],
                          e.get('list', e.get('set', [{}]))[0]]
                        for e in aggregate(aggr)])
        else:
            return aggregate(aggr)

    @classmethod
    def _ix(cls, specials, total):
        if specials._ix < 0:
            _ix = max(total + specials._ix, 0)
        else:
            _ix = min(specials._ix, total-1)

        specials._start = _ix
        specials._end = _ix + 1
        specials._limit = 1

    @classmethod
    def get_collection(cls, **params):
        params = dictset(params)
        log.debug('cls: %s, params: %s', cls.__name__, params)

        params, specials = prep_params(params)

        query_set = cls.objects
        query_set = query_set(**params)
        _total = query_set.count()

        if specials._frequencies:
            return cls.get_frequencies(query_set, specials)

        elif specials._group:
            return cls.get_group(query_set, specials)

        elif specials._distinct:
            return cls.get_distinct(query_set, specials)

        if specials._count:
            return _total

        if specials._sort:
            query_set = query_set.order_by(*specials._sort)

        if specials._ix is not None:
            cls._ix(specials, _total)

        if specials._end is None:
            query_set = query_set[specials._start:]
        else:
            query_set = query_set[specials._start:specials._end]

        if specials._scalar:
            return query_set.scalar(*specials.aslist('_scalar'))

        if specials._fields:
            only, exclude = process_fields(specials._fields).mget(['only', 'exclude'])

            if only:
                query_set = query_set.only(*only)
            elif exclude:
                query_set = query_set.exclude(*exclude)

        query_set._total = _total
        log.debug('_query: %s', query_set._query)

        if specials._explain and isinstance(query_set, mongo.QuerySet):
            return query_set.explain()

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

    def update_with(self, _dict, overwrite=True, flatten_first=False):
        self_dict = dictset(self._data)

        if flatten_first:
            self_dict = self_dict.flat()
            _dict = dictset(_dict).flat()

        for key, val in _dict.items():
            if overwrite or key not in self_dict:
                self_dict[key] = val

        for key, val in self_dict.unflat().items():
            setattr(self, key, val)

        return self

    def merge_with(self, _dict, flatten_first=False):
        return self.update_with(_dict, overwrite=False,
                            flatten_first=flatten_first)

    def to_dict(self, fields=None):
        _d = dictset(self.to_mongo().to_dict())

        if '_id' in _d:
            _d['id']=_d.pop('_id')

        if fields:
            _d = dictset(_d).extract(fields)

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
    def to_dicts(cls, keyname, **params):

        def to_dict(_d, fields):
            if isinstance(_d, dict):
                return dictset(_d).subset(fields)
            else:
                return _d.to_dict(fields=fields)

        params = dictset(params)
        _fields = params.aslist('_fields', default=[])

        if len(_fields) == 1: #if one field, assign the value directly
            _d = dictset()
            for e in cls.get_collection(**params):
                _d[e[keyname]] = getattr(e, _fields[0])
            return _d
        else:
            return dictset([[e[keyname], to_dict(e, _fields)]
                        for e in cls.get_collection(**params)])

    @classmethod
    def to_distincts(cls, fields, reverse=False):
        _d = dictset()
        fields = split_strip(fields)
        for fld in fields:
            _d[fld] = sorted(cls.objects.distinct(fld), reverse=reverse)

        return _d

    @classmethod
    def get_collection_qs(cls, qs):
        return cls.get_collection(**qs2dict(qs))


class Base(BaseMixin, mongo.Document):
    __metaclass__ = TopLevelDocumentMetaclass

    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(Base, self).update(*arg, **to_dunders(kw))
        return result


class DynamicBase(BaseMixin, mongo.DynamicDocument):
    __metaclass__ = TopLevelDocumentMetaclass

    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(DynamicBase, self).update(*arg, **to_dunders(kw))
        return result
