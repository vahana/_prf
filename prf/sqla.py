import logging
from sqlalchemy import engine_from_config
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import Column, DateTime, Integer, String

from prf.utils import dictset

log = logging.getLogger(__name__)

Session = scoped_session(sessionmaker())


def includeme(config):
    Settings = dictset(config.registry.settings)
    engine = engine_from_config(Settings, 'sqlalchemy.')

    Session.configure(bind=engine)
    BaseDocument.metadata.bind = engine


@as_declarative()
class BaseDocument(object):

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True)

    @classmethod
    def get_collection(cls, **params):
        return Session.query(cls).all()

    @classmethod
    def get_resource(cls, **params):
        return Session.query(cls).one()

    @classmethod
    def get(cls, **params):
        return cls.get_resource(__raise_on_empty=kw.pop('__raise', False),
                                 **params)

    def to_dict(self, **kw):
        return {'id': self.id}
