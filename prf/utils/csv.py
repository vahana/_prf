import sys
import logging
from datetime import datetime, date
from slovar import slovar
from slovar.strings import split_strip
import prf.exc as prf_exc
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

    if not data:
        return None

    headers = []

    if fields:
        for each in split_strip(fields):
            aa, _, bb = each.partition('__as__')
            name = (bb or aa).split(':')[0]
            headers.append(name)
    else:
        #get the headers from the first item in the data.
        #Note, data IS schemaless, so other items could have different fields.
        headers = sorted(list(data[0].flat(keep_lists=0).keys()))
        log.warn('Default headers take from the first item: %s', headers)

    tabdata = tablib.Dataset(headers=None if skip_headers else headers)

    try:
        for each in data:
            each = each.extract(headers).flat(keep_lists=0)
            row = []
            if processor:
                each = processor(each)

            # if auto_headers:
            #     new_cols = list(set(each.keys()) - set(headers))
            #     for new_col in new_cols:
            #         tabdata.append_col([None]*len(tabdata), header=new_col)

            for col in headers:
                row.append(render(each, col))

            tabdata.append(row)

        return getattr(tabdata, format_)

    except Exception as e:
        log.error('Headers:%s, auto_headers:%s, Format:%s\nData:%s', tabdata.headers, auto_headers, format_, each)
        raise prf_exc.HTTPBadRequest('dict2tab error: %r' % e)


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
            raise prf_exc.HTTPBadRequest('Unsupported Accept Header `%s`' % request.accept)

        return dict2tab(value.get('data', []),
                        fields=specials.aslist('_csv_fields', default=[]),
                        format_=_format)
