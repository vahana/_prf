import uuid
from marshmallow import Schema, fields
from prf.utils import dictset
import prf.exc


class UUID(fields.UUID):
    "This class makes sure UUID passed as objects are str'd"
    def _deserialize(self, value):
        return super(UUID, self)._deserialize(value = str(value))


class BaseSchema(Schema):

    _model = None

    def make_object(self, data):
        return self._model(**data)

    def serialize(self, request, obj):
        self.context = {'request':request}
        return dictset(self.dump(obj).data)

@BaseSchema.error_handler
def handle_errors(schema, errors, obj):
    raise prf.exc.HTTPBadRequest(errors, extra={'model':schema._model.__name__})
