import logging
from pprint import pformat
from datetime import datetime

import pymongo
from pymongo.errors import PyMongoError, BulkWriteError
from bson import ObjectId, DBRef
import mongoengine as mongo
from mongoengine.base import TopLevelDocumentMetaclass as TLDMetaclass

from slovar import slovar
import prf.exc
from prf.utils import split_strip, pager,\
                      to_dunders, process_fields, qs2dict, parse_specials, typecast, Params
from prf.renderers import _JSONEncoder
import collections


log = logging.getLogger(__name__)


class CommandLogger(pymongo.monitoring.CommandListener):
    def started(self, event):
        log.debug("Command {0.command_name} with request id "
                     "{0.request_id} started on server "
                     "{0.connection_id}".format(event))
    def succeeded(self, event):
        log.debug("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds".format(event))
    def failed(self, event):
        log.error("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds".format(event))

# pymongo.monitoring.register(CommandLogger())


class TopLevelDocumentMetaclass(TLDMetaclass):

    def __new__(cls, name, bases, attrs):
        super_new = super(TopLevelDocumentMetaclass, cls)
        attrs_meta = slovar(attrs.get('meta', {}))
        new_klass = super_new.__new__(cls, name, bases, attrs)

        if attrs_meta.pop('enable_signals', False):
            for signal in mongo.signals.__all__:
                if hasattr(new_klass, signal):
                    method = getattr(new_klass, signal)
                    if isinstance(method, collections.Callable):
                        getattr(mongo.signals, signal).connect(method, sender=new_klass)

        return new_klass


def get_document_cls(name, _raise=True):
    try:
        return mongo.document.get_document(name)
    except Exception as e:
        if _raise:
            raise ValueError('`%s` document does not exist' % name)


def drop_collections(name_prefix):
    db = mongo.connection.get_db()
    for name in db.collection_names():
        if name.startswith(name_prefix):
            log.warning('dropping `%s` collection' % name)
            db.drop_collection(name)

def drop_db(name):
    return mongo.connect().drop_database(name)

def includeme(config):
    mongo_connect(config.prf_settings())

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
    settings = slovar(settings)
    db = settings['mongodb.db']
    host = settings.get('mongodb.host', 'localhost')
    port = settings.asint('mongodb.port', default=27017)
    alias = settings.get('mongodb.alias', 'default')

    mongo.connect(db=db, host=host, port=port, alias=alias, connect=False)
    log.info('MongoDB enabled with db:%s, host:%s, port:%s, alias:%s', db, host, port, alias)


def mongo_disconnect(alias):
    mongo.connection.disconnect(alias)

def is_exists_error(e):
    return 'E11000' in str(e)

def mongodb_exc_tween(handler, registry):
    log.info('mongodb_exc_tween enabled')

    def tween(request):
        try:
            return handler(request)

        except mongo.NotUniqueError as e:
            if is_exists_error(e):
                raise prf.exc.HTTPConflict(detail='Resource already exists.',
                            request=request, exception=e)
            else:
                raise prf.exc.HTTPBadRequest(e, request=request)

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
        self._agg = []
        self.specials = specials
        self.match_query = slovar(query)
        self.accumulators = []

        if specials.get('_group'):
            self.setup_group()

        elif specials.get('_unwind'):
            self.setup_unwind()

    @staticmethod
    def undot(name):
        return name.replace('.', '__')

    def setup_group(self):
        self.specials.aslist('_group')
        self.accumulators = []

        self.count_cond = self.group_count_cond()

        for name,val in list(self.specials.items()):
            if name.startswith('_group_count'):
                self.specials.asint(name)
                continue
            elif name.startswith('_group_'):
                op = name[7:]
                if op not in ['list', 'set']:
                    for _v in split_strip(val):
                        self.accumulators.append([op, _v])
                else:
                    self.accumulators.append([op, val])

    def setup_unwind(self):
        self.post_match = slovar()

        for name, val in list(self.match_query.items()):
            if name.startswith(self.specials._unwind):
                self.post_match[name] = val

        self.match_query.pop_many(list(self.post_match.keys()))

    def unwind(self, collection):

        if self.match_query:
            self._agg.append({'$match': self.match_query})

        self._agg.append({'$unwind': {
                'path': '$%s' % self.specials._unwind
            }})

        if self.post_match:
            self._agg.append({'$match': self.post_match})

        if self.specials.asbool('_count', False):
            return self.aggregate_count(collection)

        self.add_sort()
        self.add_skip()
        self.add_limit()

        return self.aggregate(collection)

    def group_count_cond(self):
        count_cond = self.match_query.pop('count', None)

        if count_cond:
            if isinstance(count_cond, dict):
                count_cond = dict(
                    [[k,int(v)] for k,v in list(count_cond.items())])
            else:
                count_cond = int(count_cond)

            return {"$match": {"count": count_cond}}

    def group(self, collection):

        if self.match_query:
            self._agg.append({'$match':self.match_query})

        if self.specials._unwind:
            self._agg.append({'$unwind': {
                            'path': '$%s' % self.specials._unwind
                        }})

        self.add_group()

        if self.count_cond:
            self._agg.append(self.count_cond)

        if self.specials.asbool('_count', False):
            return self.aggregate_count(collection)

        self.add_group_project()
        self.add_sort(['-count'])
        self.add_skip()
        self.add_limit()
        return self.aggregate(collection)


    def add_group(self):
        group_dict = {}

        for each in self.specials._group:
            group_dict[self.undot(each)] = '$%s' % each

        if group_dict:
            _d = {'_id': group_dict,
                  'count': {'$sum':1}}

            for (op, val) in self.accumulators:
                _op = op.lower()
                if _op in ['addtoset', 'set']:
                    sfx = 'set'
                    op = '$addToSet'
                elif _op in ['push', 'list']:
                    sfx = 'list'
                    op = '$push'
                else:
                    sfx = self.undot('%s_%s' % (val, op))
                    op = '$%s'%op

                if val == '$ROOT':
                    _d[sfx] = {op:'$$ROOT'}
                    continue

                _dd = {}
                if sfx in ['set', 'list']:
                    for _v in split_strip(val):
                        _v,_,n2 = _v.partition('__as__')
                        _dd[self.undot(n2 or _v)] = '$%s' % _v
                else:
                    _dd = '$%s' % val

                _d[sfx] = {op:_dd}

            self._agg.append({'$group':_d})

        return self

    def add_group_project(self):
        #_id_ field will be used in sort
        _prj = {'count':1}

        for each in self.specials._group:
            _prj[each] = '$_id.%s' % self.undot(each)

        proj_keys = {}
        for each in self._agg:
            if '$group' in each:
                proj_keys = list(each['$group'].keys())

        for each in proj_keys:
            if each == '_id':
                continue
            for _v in split_strip(each):
                _prj[_v] = '$%s' % self.undot(_v)

        if '_group_count' in self.specials:
            for op in ['list', 'set']:
                if op in _prj:
                    _prj[op] = {"$slice": ["$%s"%op, self.specials._group_count]}

        self._agg.append(
            {'$project': _prj}
        )

        return self

    def add_sort(self, default=[]):
        from bson.son import SON
        _sort = []

        for each in self.specials._sort or default:
            if each[0] == '-':
                _sort.append((each[1:], -1))
            else:
                _sort.append((each, 1))

        if _sort:
            #add a tie-breaker sorting so pagination does not show the same group twice
            _sort.append(('_id', -1))
            self._agg.append({'$sort':SON(_sort)})

        return self

    def add_skip(self):
        return self._agg.append({'$skip':self.specials._start})

    def add_limit(self):
        if self.specials._end is not None:
            self._agg.append({'$limit':self.specials._limit})

        return self

    def aggregate(self, collection):
        log.debug('AGG: %s', pformat(self._agg))
        try:
            return [slovar(e) for e in
                    collection.aggregate(self._agg, cursor={}, allowDiskUse=True)]
        except PyMongoError as e:
            raise prf.exc.HTTPBadRequest(e)

    def aggregate_count(self, collection):
        self._agg.append({'$group': { '_id': None, 'count': {'$sum': 1}}})
        result = self.aggregate(collection)
        if result:
            return result[0]['count']
        else:
            return 0

class BaseMixin(object):

    Q = mongo.Q
    _pk_field = ['id']

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
    def insert_many(cls, objs, fail_on_error=True):
        try:
            result = cls._get_collection().insert_many([it.to_mongo() for it in objs], ordered=False)
            return len(result.inserted_ids), []

        except BulkWriteError as e:
            if fail_on_error:
                raise

            return e.details['nInserted'], e.details['writeErrors']

    @classmethod
    def insert_into(cls, name, docs=None, query=None):
        if query:
            docs = [d.to_dict(query.get('_fields')) for
                        d in cls.get_collection(**query)]

        with mongo.context_managers.switch_collection(cls, name) as kls:
            for each in docs:
                kls(**each).save()

    @classmethod
    def get_frequencies(cls, queryset, specials):
        specials.asstr('_frequencies',  allow_missing=True)
        specials.asbool('_fq_normalize',  default=False)

        reverse = not bool(specials._sort and specials._sort[0].startswith('-'))
        for each in  sorted(
            list(queryset.item_frequencies(specials._frequencies,
            normalize=specials.asbool('_fq_normalize', default=False)
        ).items()),
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

        dset = sorted([it for it in queryset.distinct(specials._distinct) if it is not None],
                        reverse=reverse)

        if specials._end is None:
            if specials._start != 0:
                dset = dset[specials._start:]
        else:
            dset = dset[specials._start: specials._end]

        if specials._fields:
            dset = [slovar({specials._fields[0]: e}) for e in dset]

        return dset

    @classmethod
    def get_group(cls, queryset, specials):
        return Aggregator(queryset._query, specials).group(cls._get_collection())

    @classmethod
    def get_unwind(cls, queryset, specials):
        return Aggregator(queryset._query, specials).unwind(cls._get_collection())

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
        params = Params(params)
        log.debug('IN: cls: %s, params: %.512s', cls.__name__, params)
        params, specials = parse_specials(params)

        if isinstance(_q, str) or not _q:
            query_set = cls.objects
        else: # needs better way to check if its a proper query object
            query_set = cls.objects(_q)

        #TODO: move it out of here. put it in get_collection_paged?
        cls.check_indexes_exist(list(params.keys())+
                [e[1:] if e.startswith('-') else e for e in specials._sort])

        query_set = query_set(**params)

        try:
            if specials._frequencies:
                return cls.get_frequencies(query_set, specials)

            elif specials._group:
                return cls.get_group(query_set, specials)

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
            if specials._explain and isinstance(query_set, mongo.QuerySet):
                return query_set.explain()

            return query_set

        finally:
           log.debug('OUT: collection: %s, query: %.512s',
                                        cls.__name__, query_set._query)

    @classmethod
    def get_resource(cls, **params):
        params['_limit']=1
        return cls.get_collection(**params).first()

    @classmethod
    def get(cls, **params):
        try:
            return cls.get_resource(**params)
        except:
            pass

    @classmethod
    def search_text(cls, text, **params):
        params.setdefault('_sort', '$text_score')
        return cls.get_collection(**params).search_text(text)

    def unique_fields(self):
        return [e['fields'][0][0] for e in self._unique_with_indexes()] \
            + [self._meta['_pager_field']]

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        try:
            return (cls.objects.get(**params), False)
        except mongo.queryset.DoesNotExist:
            defaults.update(params)
            return (cls(**defaults).save(), True)

    @classmethod
    def get_total(cls, **params):
        return cls.get_collection(_count=1, **params)

    def repr_parts(self):
        return []

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        for pk in self._pk_field:
            parts.append('%s=%s' % (pk, getattr(self, pk, None)))

        parts.extend(self.repr_parts())
        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        return cls.get_collection(id__in=ids, _limit=len(ids), **params)

    @property
    def id_str(self):
        return str(self.id)

    def update_with(self, _dict, **kw):
        self_dict = slovar.to(self.to_dict()).update_with(_dict, **kw)

        for key, val in list(self_dict.unflat().items()):
            setattr(self, key, val)

        return self

    def to_dict(self, fields=None):
        _d = slovar.to(self.to_mongo().to_dict())

        if '_id' in _d:
            _d['id']=_d.pop('_id')

        if fields:
            _d = _d.extract(fields)

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
                return slovar(_d).subset(fields)
            else:
                return _d.to_dict(fields=fields)

        params = slovar(params)
        _fields = params.aslist('_fields', default=[])

        if len(_fields) == 1: #if one field, assign the value directly
            _d = slovar()
            for e in cls.get_collection(**params):
                _d[e[keyname]] = getattr(e, _fields[0])
            return _d
        else:
            return slovar([[e[keyname], to_dict(e, _fields)]
                        for e in cls.get_collection(**params)])

    @classmethod
    def to_distincts(cls, fields, reverse=False):
        _d = slovar()
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
        return not other or self.to_dict(list(other.keys())) == other

    def save(self, _dont_fail_on_duplicate=False):
        try:
            return super().save()
        except mongo.NotUniqueError as e:
            if _dont_fail_on_duplicate and is_exists_error(e):
                pass
                # log.debug('Found duplicate. Insert mode, skipping: %s', e)
            else:
                raise

    def save_safe(self):
        try:
            return self.save()
        except:
            import sys
            e = sys.exc_info()
            log.error('%s\nclass:<%s>\ndata:%s' %
                            (e[1], self.__class__.__name__, self.to_dict()))

    @classmethod
    def get_collection_paged(cls, page_size, **params):
        params = slovar(params or {})
        _start = int(params.pop('_start', 0))
        _limit = int(params.pop('_limit', -1))
        pager_field= params.pop('_pagination', None)

        if _limit == -1:
            _limit = cls.get_collection(_limit=_limit, _count=1, **params)

        log.debug('page_size=%s, _start=%s, _limit=%s',
                                            page_size, _start, _limit)

        def process_pagination(start, count, collection):
            if params.get('_sort') or not pager_field:
                return params.update_with({'_start':start, '_limit': count})

            if not collection:
                return params.update_with({'_sort':pager_field, '_start':start, '_limit': count})
            else:
                last_id = collection[len(collection)-1][pager_field]
                return params.update_with({
                     '_sort': pager_field,
                     '%s__gt' % pager_field: last_id,
                     '_limit': count
                    })

        pgr = pager(_start, page_size, _limit)
        results = []
        for start, count in pgr():
            _params = process_pagination(start, count, results)
            results = cls.get_collection(**_params)
            yield results

    @classmethod
    def unregister(cls):
        mongo.base._document_registry.pop(cls.__name__, None)

    @classmethod
    def rename(cls, fields, **params):
        update = {}
        renames = {}

        for current, new in list(fields.items()):
            params['%s__exists' % current] = 1
            if not new:
                update['unset__%s' % current] = 1
            else:
                renames[current] = new

        if renames:
            update['__raw__'] = {'$rename': renames}

        if update:
            return cls.objects(**params).update(**update)

    @classmethod
    def _get_indexes(cls):
        _indexes = []
        try:
            for each in list(cls._get_collection().index_information().values()):
                key = each['key'][0][0]
                if key == '_id':
                    key = 'id'
                _indexes.append(key)
        except pymongo.errors.OperationFailure:
            return None

        return _indexes

    @classmethod
    def check_indexes_exist(cls, keys):
        existing_indexes = cls._get_indexes()
        if existing_indexes is None:
            return

        new_keys = []
        for kk in keys:
            parts = kk.split('__')
            if parts[-1] in ['exists', 'in', 'ne', 'size',
                             'asbool', 'asint', 'aslist', 'asdt', 'gt', 'lt', 'gte', 'lte']:
                parts.pop(-1)

            new_keys.append('.'.join(parts))

        missing = set(new_keys) - set(existing_indexes)
        if missing:
            log.warning('Missing indexes for the query on `%s`: %s',
                        cls.__name__, list(missing))

    @classmethod
    def mark_dups(cls, keys, page_size=100, **query):
        total_marked = 0

        for batch in cls.get_collection_paged(
                        page_size,
                        dups_by__exists=0,
                        count__gt=1,
                        _group=keys,
                        _group_list='_id',
                        **query):

            dup_ids = []

            for each in batch:
                ids = [e['_id'] for e in each['list']]
                dup_ids.extend(sorted(ids, reverse=True)[1:])

            total_marked += cls.objects(id__in=dup_ids)\
                               .update(set__dups_by=keys,
                                       write_concern={"w": 1})

            log.debug('%s marked as dups by %s', total_marked, keys)

    def get_density(self, fields=[]):
        return len(self.to_dict(fields).flat(keep_lists=1))


class Base(BaseMixin, mongo.Document, metaclass=TopLevelDocumentMetaclass):
    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(Base, self).update(*arg, **to_dunders(kw))
        return result


class DynamicBase(BaseMixin, mongo.DynamicDocument, metaclass=TopLevelDocumentMetaclass):
    meta = {'abstract': True}

    def update(self, *arg, **kw):
        result = super(DynamicBase, self).update(*arg, **to_dunders(kw))
        return result


