import logging
from sqlalchemy import engine_from_config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

from prf.utils import dictset, DataProxy, split_strip, process_limit
from prf.json_httpexceptions import *

log = logging.getLogger(__name__)

Session = scoped_session(sessionmaker())


def includeme(config):
    Settings = dictset(config.registry.settings)
    engine = engine_from_config(Settings, 'sqlalchemy.')

    Session.configure(bind=engine)
    BaseDocument.metadata.bind = engine


@as_declarative()
class BaseDocument(object):

    _type = property(lambda self: self.__class__.__name__)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)

    def to_dict(self, request=None, **kw):
        _data = dictset({c.name: getattr(self, c.name)
                        for c in self.__table__.columns})
        _data['_type'] = self._type
        _data.update(kw.pop('override', {}))
        return DataProxy(_data).to_dict(**kw)

    def save(self, commit=True):
        Session.add(self)
        try:
            Session.commit()
        except IntegrityError as e:
            Session.rollback()

            if 'unique' in e.message.lower():
                raise JHTTPConflict('Resource `%s` already exists.'
                                % self.__class__.__name__, extra={'data': e})

            else:
                raise JHTTPBadRequest("Resource `%s` could not be created: missing params"
                                  % (self.__class__.__name__),
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
        except IntegrityError as e:
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

        if _limit is None:
            raise KeyError('Missing _limit')

        return params, locals()

    @classmethod
    def get_collection(cls, **params):
        params, specials = cls.prep_params(params)

        query_set = Session.query(cls).filter_by(**params)

        _start, _limit = process_limit(specials['_start'],
                        specials['_page'], specials['_limit'])

        _total = query_set.count()

        query_set = query_set.offset(_start).limit(_limit)

        if specials['_count']:
            return _total

        query_set._prf_meta = dict(total=_total, start=_start,
                            fields=specials['_fields'])
        return query_set

    @classmethod
    def get_resource(cls, _raise=True, **params):
        params['_limit'] = 1
        params, specials = cls.prep_params(params)
        try:
            obj = Session.query(cls).filter_by(**params).one()
            obj._prf_meta = dict(fields=specials['_fields'])
            return obj

        except NoResultFound as e:
            msg = "'%s(%s)' resource not found" % (cls.__name__, params)
            if _raise:
                raise JHTTPNotFound(msg)
            else:
                log.debug(msg)
                return None

    @classmethod
    def get(cls, **params):
        return cls.get_resource(_raise=False, **params)
