import json
import logging
from datetime import date, datetime

log = logging.getLogger(__name__)

class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ") #iso
        try:
            return super(_JSONEncoder, self).default(obj)
        except TypeError:
            return unicode(obj) #fallback to unicode

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
