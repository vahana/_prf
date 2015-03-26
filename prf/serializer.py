import uuid
from marshmallow import Schema, fields
from prf.utils import dictset
import prf.exc


class UUIDType(fields.UUID):

    """This class makes sure UUID passed as objects are str'd"""

    def _deserialize(self, value):
        return super(UUIDType, self)._deserialize(value=str(value))


class BaseSchema(Schema):

    _model = None

    def make_object(self, data):
        return self._model(**data)


@BaseSchema.error_handler
def handle_errors(schema, errors, obj):
    raise prf.exc.HTTPBadRequest(errors,
                                 extra={'model': schema._model.__name__})
