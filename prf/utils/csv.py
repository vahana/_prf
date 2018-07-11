import sys
import logging
from datetime import datetime, date
from slovar import slovar
from slovar.strings import split_strip
from prf.exc import HTTPBadRequest

log = logging.getLogger(__name__)

def dict2tab(data, fields=None, format_='csv', skip_headers=False):
    import tablib

    def _pop(each, key):
        val = each.pop(key, '')
        if isinstance(val, (datetime, date)):
            return val.strftime('%Y-%m-%dT%H:%M:%SZ')  # iso
        else:
            return str(val)

    headers = []
    fields = fields or []
    data = data or []

    for each in split_strip(fields):
        aa, _, bb = each.partition('__as__')
        name = (bb or aa).split(':')[0]
        headers.append(name)

    tabdata = tablib.Dataset(headers = None if skip_headers else headers)
    try:
        for each in data:
            row = []
            each = slovar(each).flat(keep_lists=0)
            for col in headers:
                row.append(_pop(each, col))

            tabdata.append(row)

        return getattr(tabdata, format_)

    except:
        log.debug('Headers:%s, Fields:%s, Format:%s', headers, fields, format_)
        raise HTTPBadRequest('dict2tab error: %r'%sys.exc_info()[1])


class TabRenderer(object):
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        response = request.response
        params = system['view'](None, request).process_params(request)

        if 'text/csv' in request.accept:
            response.content_type = 'text/csv'
            _format = 'csv'
        elif 'text/xls' in request.accept:
            _format = 'xls'
        else:
            raise HTTPBadRequest('Unsupported Accept Header `%s`' % request.accept)

        return dict2tab(value.get('data', []),
                        fields=params.get('_fields'),
                        format_=_format)
