import logging
import uuid

import pyramid
from sqlalchemy import engine_from_config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from prf.utils import dictset, DataProxy, split_strip, process_limit
from prf.json_httpexceptions import *

log = logging.getLogger(__name__)

Session = scoped_session(sessionmaker())


def includeme(config):
    config.add_tween('prf.sqla.sqla_exc_tween', over=pyramid.tweens.MAIN)

    Settings = dictset(config.registry.settings)
    engine = engine_from_config(Settings, 'sqlalchemy.')

    Session.configure(bind=engine)


def sqla_exc_tween(handler, registry):
    def exc_dict(e):
        return {'class': e.__class__, 'message': e.message}

    def exc(request):
        try:
            return handler(request)
        except SQLAlchemyError, e:
            Session.rollback()
            raise JHTTPBadRequest('', request=request, exception=exc_dict(e))

    return exc


def order_by_clauses(model, _sort):
    _sort_param = []

    def _raise(attr):
        raise JHTTPBadRequest("Resource `%s` has not attribute `%s`" %
                              (model.__name__, attr))

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


class BaseMixin(object):

    _type = property(lambda self: self.__class__.__name__)

    def __init__(self, **kw):
        self.__dict__.update(kw)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    def to_dict(self, request=None, **kw):
        _data = dictset({c.name: getattr(self, c.name)
                        for c in self.__table__.columns})
        _data['_type'] = self._type
        _data.update(kw.pop('override', {}))
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
        Session.add(self)
        try:
            Session.commit()
        except IntegrityError, e:
            raise
            Session.rollback()
            if 'unique' in e.message.lower():
                raise JHTTPConflict('Resource `%s` already exists.'
                                    % self.__class__.__name__,
                                    extra={'data': e})
            else:

                raise JHTTPBadRequest('Resource `%s`: %s'
                                      % (self.__class__.__name__, e.message),
                                      extra={'data': e})

        return self

    def _update(self, params, **kw):
        for key, value in params.items():
            setattr(self, key, value)

        return self.save(**kw)

    def delete(self):
        Session.delete(self)
        try:
            Session.commit()
        except IntegrityError, e:
            Session.rollback()

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
    def objects(cls, **params):
        params, specials = cls.prep_params(params)
        return Session.query(cls).filter_by(**params)

    @classmethod
    def get_collection(cls, **params):
        params, specials = cls.prep_params(params)

        if specials['_limit'] is None:
            raise KeyError('Missing _limit')

        query = Session.query(cls)

        if params:
            query = Session.query(cls).filter_by(**params)

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
        params['_limit'] = 1
        params, specials = cls.prep_params(params)
        try:
            obj = Session.query(cls).filter_by(**params).one()
            obj._prf_meta = dict(fields=specials['_fields'])
            return obj
        except NoResultFound, e:

            msg = "'%s(%s)' resource not found" % (cls.__name__, params)
            if _raise:
                raise JHTTPNotFound(msg)
            else:
                log.debug(msg)
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
