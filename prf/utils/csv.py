import sys
import logging
from datetime import datetime, date
from slovar import slovar
from slovar.strings import split_strip
from prf.exc import HTTPBadRequest
from prf.utils import maybe_dotted

log = logging.getLogger(__name__)

def dict2tab(data, fields=None, format_='csv', skip_headers=False, processor=None, auto_headers=False):
    import tablib

    if processor:
        processor = maybe_dotted(processor, throw=True)

    def render(each, key):
        val = each.get(key)
        if isinstance(val, (datetime, date)):
            val = val.strftime('%Y-%m-%dT%H:%M:%SZ')  # iso

        return val

    data = data or []
    headers = []

    if auto_headers and data:
        headers = list(data[0].flat(keep_lists=0).keys())
    elif fields:
        for each in split_strip(fields):
            aa, _, bb = each.partition('__as__')
            name = (bb or aa).split(':')[0]
            headers.append(name)

    tabdata = tablib.Dataset(headers=None if skip_headers else headers)

    try:
        for each in data:
            each = each.flat(keep_lists=0)
            row = []
            if processor:
                each = processor(each)

            if auto_headers:
                new_cols = list(set(each.keys()) - set(tabdata.headers))
                for new_col in new_cols:
                    tabdata.append_col([None]*len(tabdata), header=new_col)

            for col in tabdata.headers:
                row.append(render(each, col))

            tabdata.append(row)

        return getattr(tabdata, format_)

    except Exception as e:
        log.error('Headers:%s, auto_headers:%s, Format:%s\nData:%s', tabdata.headers, auto_headers, format_, each)
        raise HTTPBadRequest('dict2tab error: %r' % e)


class TabRenderer(object):
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        response = request.response
        _, specials = system['view'](None, request).process_params(request)

        if 'text/csv' in request.accept:
            response.content_type = 'text/csv'
            _format = 'csv'
        elif 'text/xls' in request.accept:
            _format = 'xls'
        else:
            raise HTTPBadRequest('Unsupported Accept Header `%s`' % request.accept)

        return dict2tab(value.get('data', []),
                        fields=specials.aslist('_fields', default=[]),
                        format_=_format,
                        auto_headers= '_auto_headers' in specials)
