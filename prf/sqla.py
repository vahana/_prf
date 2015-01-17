import re
import logging

from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm.exc import NoResultFound
import sqlalchemy.exc as sqla_exc
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.properties import ColumnProperty

from prf.utils import dictset, DataProxy, split_strip, process_limit
import prf.json_httpexceptions as prf_exc

log = logging.getLogger(__name__)


def dbi2http(code, exc):
    def exc_dict(e):
        return {'class': e.__class__, 'message': e.message}

    if code == '23502':
        match = re.search(r'"([A-Za-z0-9_\./\\-]*)"', exc.message)
        msg = 'Missing param %s' % match.group()
        return prf_exc.JHTTPBadRequest(msg, exception=exc_dict(exc))
    elif code == '23503':
        msg = 'Can not update or delete resource: child resource(s) exist'
        return prf_exc.JHTTPConflict(msg, exception=exc_dict(exc))
    elif code == '23505':
        msg = "Resource already exists"
        return prf_exc.JHTTPConflict(msg, exception=exc_dict(exc))

    else:
        return prf_exc.JHTTPServerError(code, exception=exc_dict(exc))


def set_db_session(config, session):
    config.registry.db_session = session

    from sqlalchemy import event
    import psycopg2

    @event.listens_for(session.bind.engine, "handle_error")
    def handle_exception(context):
        if isinstance(context.original_exception, psycopg2.DatabaseError):
            raise dbi2http(context.original_exception.pgcode,
                           context.original_exception)


def db(request):
    try:
        return request.registry.db_session
    except AttributeError as e:
        raise AttributeError(
                "%s: Make sure to call config.set_db_session(session)" % e)


def includeme(config):
    import pyramid
    config.add_tween('prf.sqla.sqla_exc_tween', over=pyramid.tweens.MAIN)
    config.add_directive('set_db_session', set_db_session)
    config.add_request_method(db, reify=True)


def sqla2http(exc, request=None):
    _, _, failed = exc.message.partition(':')
    _, _, param = failed.partition('.')

    def exc_dict(e):
        return {'class': e.__class__, 'message': e.message}

    if isinstance(exc, NoResultFound):
        return prf_exc.JHTTPNotFound(request=request, exception=exc_dict(exc))

    elif isinstance(exc, sqla_exc.InvalidRequestError) and 'has no property' \
        in exc.message.lower():

        return prf_exc.JHTTPBadRequest('Bad params', request=request,
                               exception=exc_dict(exc))

    elif isinstance(exc, sqla_exc.SQLAlchemyError):
        import traceback
        log.error(traceback.format_exc())
        return prf_exc.JHTTPBadRequest('SQLA error', request=request,
                                                    exception=exc_dict(exc))

    else:
        return exc


def sqla_exc_tween(handler, registry):
    log.info('sqla_exc_tween enabled')

    def tween(request):
        try:
            resp = handler(request)
            request.db.commit()
            return resp

        except sqla_exc.SQLAlchemyError, e:
            request.db.rollback()
            raise sqla2http(e)

        except:
            request.db.rollback()
            import traceback
            log.error(traceback.format_exc())
            raise

    return tween


def order_by_clauses(model, _sort):
    _sort_param = []

    def _raise(attr):
        raise AttributeError("Bad attribute '%s'" % attr)

    for each in split_strip(_sort):
        if each.startswith('-'):
            each = each[1:]
            attr = getattr(model, each, None)
            if not attr:
                _raise(each)

            _sort_param.append(attr.desc())
            continue
        elif each.startswith('+'):

            each = each[1:]

        attr = getattr(model, each, None)
        if not attr:
            _raise(each)

        _sort_param.append(attr.asc())

    return _sort_param


class Base(object):

    _type = property(lambda self: self.__class__.__name__)

    @classmethod
    def get_session(cls):
        raise NotImplementedError('Must return a session')

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @property
    def column_names(self):
        return set(self.__table__.columns.keys())

    @property
    def field_names(self):
        return self.column_names

    def to_dict(self, request=None, **kw):
        def get_data():
            data = dictset()
            fields = self.field_names | set(kw.get('fields', []))
            for attr in fields:
                data[attr] = getattr(self, attr)

            return data

        _data = get_data()
        _data['_type'] = self._type
        return DataProxy(_data).to_dict(**kw)

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

    def save(self, commit=True):
        session = self.get_session()
        session.add(self)
        return self

    def update(self, **params):
        for key, value in params.items():
            setattr(self, key, value)

        return self.save()

    def delete(self):
        session = self.get_session()
        session.delete(self)

    @classmethod
    def prep_params(cls, params):
        params = dictset(params)

        __confirmation = '__confirmation' in params
        params.pop('__confirmation', False)

        _sort = split_strip(params.pop('_sort', []))
        _fields = split_strip(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)
        _count = '_count' in params
        params.pop('_count', None)

        return params, locals()

    @classmethod
    def query(cls, *args, **kw):
        return cls.get_session().query(cls, *args, **kw)

    @classmethod
    def objects(cls, **params):
        params, specials = cls.prep_params(params)
        return cls.get_session().query(cls).filter_by(**params)

    @classmethod
    def get_collection(cls, *args, **params):
        session = cls.get_session()
        params, specials = cls.prep_params(params)

        if specials['_limit'] is None:
            raise KeyError('Missing _limit')

        query = session.query(cls)

        if args:
            query = query.filter(*args)

        if params:
            query = session.query(cls).filter_by(**params)

        if specials['_sort']:
            query = query.order_by(*order_by_clauses(cls, specials['_sort']))

        start, limit = process_limit(specials['_start'], specials['_page'],
                                     specials['_limit'])

        total = query.count()

        query = query.offset(start).limit(limit)

        if specials['_count']:
            return total

        query._prf_meta = dict(total=total, start=start,
                               fields=specials['_fields'])

        return query

    @classmethod
    def get_resource(cls, _raise=True, **params):
        session = cls.get_session()
        params['_limit'] = 1
        params, specials = cls.prep_params(params)
        try:
            obj = session.query(cls).filter_by(**params).one()
            obj._prf_meta = dict(fields=specials['_fields'])
            return obj
        except NoResultFound, e:
            if _raise:
                raise
            else:
                log.debug("'%s(%s)' resource not found" % (cls.__name__, params))
                return None

    @classmethod
    def get(cls, **params):
        return cls.get_resource(_raise=False, **params)

    @classmethod
    def get_or_create(cls, **kw):
        params = kw.pop('defaults', {})
        params.update(kw)
        obj = cls.get(**kw)
        if obj:
            return obj, False
        else:
            return cls(**params).save(), True
