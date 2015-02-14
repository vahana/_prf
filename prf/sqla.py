import re
import logging

import sqlalchemy as sa
import sqlalchemy.exc as sqla_exc
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import class_mapper, exc as orm_exc
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy_utils import get_columns, get_hybrid_properties, get_mapper
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import event

from prf.utils import dictset, split_strip, process_limit, prep_params,\
                      maybe_dotted
import prf.exc

log = logging.getLogger(__name__)


def db(request):
    session = request.registry.dbsession

    def cleanup(request):
        if request.exception is not None:
            session.rollback()
        else:
            session.commit()
        session.close()
    request.add_finished_callback(cleanup)

    return session


def includeme(config):
    import pyramid
    config.add_tween('prf.sqla.sqla_exc_tween',
                      under='pyramid.tweens.excview_tween_factory')

    config.add_request_method(db, reify=True)


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
            raise

    return tween


def init_session(config, db_url, model, session):
    config.registry.dbsession = session

    engine = sa.create_engine(db_url)
    session.configure(bind=engine)

    model = maybe_dotted(model)
    model.metadata.bind = engine
    model.Session = session

    @event.listens_for(engine, "handle_error")
    def handle_exception(context):
        try:
            import psycopg2
            if isinstance(context.original_exception, psycopg2.DatabaseError):
                raise postgres2http(context.original_exception.pgcode,
                                    context.original_exception)
        except ImportError:
            pass
            #not psycopg2

        session.rollback()
        raise sqla2http(context)

    return model


def sqla2http(exc, request=None):
    def exc_dict(e):
        return e.message

    if isinstance(exc, orm_exc.NoResultFound):
        return prf.exc.HTTPNotFound(request=request, exception=exc_dict(exc))

    elif isinstance(exc, sqla_exc.InvalidRequestError) and 'has no property' \
        in exc.message.lower():

        return prf.exc.HTTPBadRequest('Bad params', request=request,
                               exception=exc_dict(exc))

    elif isinstance(exc, sqla_exc.SQLAlchemyError):
        import traceback
        log.error(traceback.format_exc())
        return prf.exc.HTTPBadRequest('SQLA error', request=request,
                                                    exception=exc_dict(exc))

    else:
        return exc


def postgres2http(code, exc):
    def exc_dict(e):
        return e.message

    if code == '23502':
        match = re.search(r'"([A-Za-z0-9_\./\\-]*)"', exc.message)
        msg = 'Missing param: %s' % match.group()
        return prf.exc.HTTPBadRequest(msg, exception=exc_dict(exc))
    elif code == '23503':
        msg = 'Can not update or delete resource: child resource(s) exist'
        return prf.exc.HTTPConflict(msg, exception=exc_dict(exc))
    elif code == '23505':
        msg = "Resource already exists"
        return prf.exc.HTTPConflict(msg, exception=exc_dict(exc))
    elif code == '22P02':
        msg = "Bad value"
        return prf.exc.HTTPBadRequest(msg, exception=exc_dict(exc))
    else:
        return prf.exc.HTTPServerError(code, exception=exc_dict(exc))


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

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

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

    def save(self):
        self.Session().add(self)
        return self

    def update(self, **params):
        for key, value in params.items():
            setattr(self, key, value)

        return self.save()

    def delete(self):
        self.Session().delete(self)

    @classmethod
    def query(cls, *args, **kw):
        return cls.Session().query(cls, *args, **kw)

    @classmethod
    def objects(cls, **params):
        params, specials = prep_params(params)
        return cls.Session().query(cls).filter_by(**params)

    @classmethod
    def get_collection(cls, *args, **params):
        params, specials = prep_params(params)
        session = cls.Session()

        query = session.query(cls)

        query = query.filter(*args) if args else session.query(cls)

        if params:
            query = session.query(cls).filter_by(**params)

        query._total = query.count()

        if specials._sort:
            query = query.order_by(*order_by_clauses(cls, specials._sort))

        return query.offset(specials._offset).limit(specials._limit)

    @classmethod
    def get_resource(cls, _raise=True, **params):
        params, _ = prep_params(params)

        try:
            obj = cls.Session().query(cls).filter_by(**params).one()
            return obj
        except orm_exc.NoResultFound, e:
            msg = "'%s(%s)' resource not found" % (cls.__name__, params)
            if _raise:
                raise prf.exc.HTTPNotFound(msg)
            else:
                log.debug(msg)

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
