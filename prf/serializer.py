from marshmallow import Schema
from prf.utils import dictset

class BaseSchema(Schema):

    _model = None

    def make_object(self, data):
        return self._model(**data)

    def serialize(self, request, obj):
        self.context = {'request':request}
        return dictset(self.dump(obj).data)
