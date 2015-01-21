import json
import logging
from datetime import date, datetime
from prf.utils import JSONEncoder as _JSONEncoder

log = logging.getLogger(__name__)


class JsonRendererFactory(object):

    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request:
            response = request.response
            ct = response.content_type
            if ct == response.default_content_type:
                response.content_type = 'application/json'

        return json.dumps(value, cls=_JSONEncoder)
