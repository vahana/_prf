import logging
from datetime import datetime
from bson import ObjectId, DBRef
import mongoengine as mongo
from mongoengine.base import TopLevelDocumentMetaclass as TLDMetaclass

import pymongo

import prf.exc
from prf.utils import dictset, split_strip, pager,\
                      to_dunders, process_fields, qs2dict
from prf.utils.qs import prep_params, typecast
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


class Aggregator(object):

    def __init__(self, query, specials):
        self.data = []
        self.specials = specials
        self.match_query = query
        self.accumulators = {}

        if specials.get('_group'):
            self.setup_group()

        elif specials.get('_join'):
            self.setup_join()

    @staticmethod
    def undot(name):
        return name.replace('.', '__')

    def setup_group(self):
        self.specials.aslist('_group')
        self.accumulators = dictset([[e[7:], self.specials[e]] \
            for e in self.specials if e.startswith('_group_')])

    def setup_join(self):
        _join,_,_join_on = self.specials._join.partition('.')
        _join_as = self.specials.asstr('_join_as', default=_join)
        self.specials.aslist('_join_fields', default=[])

        self.after_match = dictset()
        for key,val in self.match_query.items():
            if key == _join_as or key.startswith('%s.' % _join_as):
                self.after_match[key]=val
                self.match_query.pop(key)

        self._join_cond = typecast(dictset([e.split(':') for e in
            self.specials.aslist('_join_cond', default=[])]))

        self.specials._join = _join
        self.specials.aslist('_join_on', default=[_join_on, _join_on])

    def unwind(self, collection):
        if self.match_query:
            self.data.append({'$match': self.match_query})

        self.data.append({'$unwind': {
                'path': '$%s' % self.specials._unwind
            }})

        if self.specials.asbool('_count', False):
            return self.aggregate_count(collection)

        self.add_sort()
        self.add_limit()

        return self.aggregate(collection)

    def group(self, collection):
        if self.match_query:
            self.data.append({'$match':self.match_query})

        self.add_unwind()
        self.add_group()

        if self.specials.asbool('_count', False):
            return self.aggregate_count(collection)

        self.add_project()
        self.add_sort({'count': -1})
        self.add_limit()
        return self.aggregate(collection)

    def join(self, collection):
        if self.match_query:
            self.data.append({'$match':self.match_query})

        self.add_lookup()

        _join_as = self.specials._join_as
        _join_on = self.specials._join_on

        if self._join_cond:
            self.data.append({'$unwind': '$%s'%_join_as})

            true_case = []
            for field, value in self._join_cond.items():
                if not field.startswith(_join_as):
                    field = '.'.join([_join_as, field])
                true_case.append({'$eq': ['$%s'%field, value]})

            _project = {_join_as:
                            {'$cond': {
                                'if': {'$and': true_case},
                                'then': '$%s'%(_join_as),
                                'else': None }},
                        '_id': 0,
                        _join_on[0]: 1
                        }

            self.data.append({'$project': _project})

            self.data.append(
                { '$group' : {
                    '_id' : "$%s"%_join_on[0],
                    _join_as: {'$push': '$%s.%s' % (_join_as, _join_on[1])},
                    }
                }
            )

            _project = {
                    _join_on[0]: '$_id',
                    _join_as: '$%s'% (_join_as),
                    '_id': 0,
                }
            self.data.append(
                {'$project': _project}
            )


        if self.after_match:
            self.data.append({'$match': self.after_match})

        if self.specials.asbool('_count', False):
            return self.aggregate_count(collection)

        self.add_sort()
        self.add_limit()

        return self.aggregate(collection)

    def add_unwind(self):
        _prj = {"_id": "$_id"}
        unwinds = []

        for op, name in self.accumulators.items():
            if op != 'unwind':
                continue

            self.accumulators.pop(op)
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
            self.data.append({'$project':_prj})
            self.data.extend(unwinds)

        return self

    def add_group(self):
        group_dict = {}

        for each in self.specials._group:
            group_dict[self.undot(each)] = '$%s' % each

        if group_dict:
            _d = {'_id': group_dict,
                  'count': {'$sum':1}}

            for op, val in self.accumulators.items():
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
                        _dd[self.undot(_v)] = '$%s' % _v
                else:
                    _dd = '$%s' % val

                _d[sfx] = {op:_dd}

            self.data.append({'$group':_d})

        return self

    def add_lookup(self):
        join_on = self.specials._join_on

        if len(join_on) == 1:
            left = right = join_on[0]
        elif len(join_on) == 2:
            left,right = join_on
        else:
            raise prf.exc.HTTPBadRequest(
                'Use `_join_on` or dotted `_join` to pass the field to join on')

        self.data.append(
            {'$lookup': {
                    'from': self.specials._join,
                    'localField': left,
                    'foreignField': right,
                    'as': self.specials._join_as
                }
            }
        )

        return self

    def add_project(self):
        _prj = {'_id':0, 'count':1}

        for each in self.specials._group:
            _prj[each] = '$_id.%s' % self.undot(each)

        _gkeys = {}
        for each in self.data:
            if '$group' in each:
                _gkeys = each['$group'].keys()

        for each in _gkeys:
            if each == '_id':
                continue
            for _v in split_strip(each):
                _prj[_v] = '$%s' % self.undot(_v)

        self.data.append(
            {'$project': _prj}
        )

        return self

    def add_sort(self, default={}):
        sort_dict = {}

        for each in self.specials._sort:
            if each[0] == '-':
                sort_dict[each[1:]] = -1
            else:
                sort_dict[each] = 1

        sort_dict = sort_dict or default
        if sort_dict:
            self.data.append({'$sort':sort_dict})

        return self

    def add_limit(self):
        self.data.append({'$skip':self.specials._start})
        if self.specials._end is not None:
            self.data.append({'$limit':self.specials._limit})

        return self

    def aggregate(self, collection):
        log.debug(self.data)
        return [e for e in collection.aggregate(self.data, cursor={}, allowDiskUse=True)]

    def aggregate_count(self, collection):
        self.data.append({'$group': { '_id': None, 'count': {'$sum': 1}}})
        result = self.aggregate(collection)
        if result:
            return result[0]['count']
        else:
            return 0

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
            if specials._start != 0:
                dset = dset[specials._start:]
        else:
            dset = dset[specials._start: specials._end]

        if specials._fields:
            dset = [dictset({specials._fields[0]: e}) for e in dset]

        return dset

    @classmethod
    def get_group(cls, queryset, specials):
        return Aggregator(queryset._query, specials).group(cls._collection)

    @classmethod
    def get_join(cls, queryset, specials):
        return Aggregator(queryset._query, specials).join(cls._collection)

    @classmethod
    def get_unwind(cls, queryset, specials):
        return Aggregator(queryset._query, specials).unwind(cls._collection)

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
    def get_collection(cls, _q=None, **params):
        params = dictset(params)

        log.debug('IN: cls: %s, params: %.512s', cls.__name__, params)

        params, specials = prep_params(params)

        if isinstance(_q, basestring) or not _q:
            query_set = cls.objects
        else: # needs better way to check if its a proper query object
            query_set = cls.objects(_q)

        query_set = query_set(**params)

        if specials._frequencies:
            return cls.get_frequencies(query_set, specials)

        elif specials._group:
            return cls.get_group(query_set, specials)

        elif specials._join:
            return cls.get_join(query_set, specials)

        elif specials._distinct:
            return cls.get_distinct(query_set, specials)

        elif specials._unwind:
            return cls.get_unwind(query_set, specials)

        _total = query_set.count()

        if specials._count:
            return _total

        if specials._sort:
            query_set = query_set.order_by(*specials._sort)

        if specials._ix is not None:
            cls._ix(specials, _total)

        if specials._end is None:
            if specials._start == 0:
                return query_set
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
        log.debug('OUT: collection: %s, query: %.512s', cls._collection.name, query_set._query)

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

    @classmethod
    def search_text(cls, text, **params):
        params.setdefault('_sort', '$text_score')
        return cls.get_collection(**params).search_text(text)

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

    def update_with(self, _dict, **kw):
        self_dict = self.to_dict().update_with(_dict, **kw)

        for key, val in self_dict.unflat().items():
            setattr(self, key, val)

        return self

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

    def contains(self, other, exclude=None):
        if not isinstance(other, dict):
            other = other.to_dict(exclude)
        return not other or self.to_dict(other.keys()) == other

    def save_safe(self):
        try:
            return self.save()
        except Exception as e:
            log.error('%s: %s' % (e, self.to_dict()))

    @classmethod
    def get_collection_paged(cls, page_size, **params):
        params = dictset(params or {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))

        if _limit == -1:
            _limit = cls.get_collection(_limit=_limit, _count=1, **params)

        pgr = pager(_start, page_size, _limit)
        for start, count in pgr():
            _params = params.copy().update({'_start':start, '_limit': count})
            yield cls.get_collection(**_params)

    @classmethod
    def unregister(cls):
        mongo.base._document_registry.pop(cls.__name__, None)

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


