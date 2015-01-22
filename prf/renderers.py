import json
from prf.utils import JSONEncoder as _JSONEncoder


class JsonRenderer(object):

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
