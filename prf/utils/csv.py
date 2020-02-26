import sys
import io
import logging
import json

from datetime import datetime, date
from slovar import slovar
from slovar.strings import split_strip
import prf.exc as prf_exc
from prf.utils import maybe_dotted

log = logging.getLogger(__name__)


def get_csv_header(file_or_buff):
    import pandas as pd
    return pd.read_csv(file_or_buff, nrows=0, engine = 'c').columns.to_list()


def get_csv_total(file_or_buff):
    import pandas as pd
    df = pd.read_csv(file_or_buff, header=[0], engine = 'c')
    return df.shape[0]


def pd_read_csv(file_or_buff, **params):
    import pandas as pd
    NA_LIST = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
                '1.#IND', '1.#QNAN', 'N/A',
                'NULL', 'NaN', 'n/a', 'nan', 'null']

    if not params.get('_header'):
        params['_header'] = get_csv_header(file_or_buff)

    #add one to skip the header, since it will be passed in `_header` param
    params['_start'] = params.setdefault('_start', 0) + 1

    #make sure if its a file object, its reset to 0
    if hasattr(file_or_buff, 'seekable'):
        file_or_buff.seek(0)

    df = pd.read_csv(
                    file_or_buff,
                    infer_datetime_format=True,
                    na_values = NA_LIST,
                    keep_default_na = False,
                    dtype=object,
                    chunksize = params.get('_limit') or 20,
                    skip_blank_lines=True,
                    engine = 'c',
                    skiprows = params.get('_start') or 1,
                    nrows = params.get('_limit'),
                    names=params['_header'])

    return df


def csv2dict(file_or_buff, processor=None, fillna=None, **params):
    df = pd_read_csv(file_or_buff, **params)

    def pop_na(row):
        _d = slovar(row.to_dict())
        if fillna is None:
            return _d.pop_by_values([''])
        else:
            return _d

    processor = processor or (lambda x:x)
    results = []

    for chunk in df:
        for each in chunk.fillna(fillna or '').iterrows():
            results.append(processor(pop_na(each[1])))

    return results


def dict2tab(data, fields=None, format_='csv', skip_headers=False):
    import tablib

    def render(each, key):
        val = each.get(key)
        if isinstance(val, (datetime, date)):
            val = val.strftime('%Y-%m-%dT%H:%M:%SZ')  # iso

        elif isinstance(val, (list, tuple)):
            val = json.dumps(val)

        if val is None:
            val = ''

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
        headers = sorted(list(data[0].flat(keep_lists=1).keys()))
        log.warn('Default headers take from the first item: %s', headers)

    tabdata = tablib.Dataset(headers=None if skip_headers else headers)

    try:
        for each in data:
            each = each.extract(headers).flat(keep_lists=1)
            row = []

            for col in headers:
                row.append(render(each, col))

            tabdata.append(row)

        return getattr(tabdata, format_)

    except Exception as e:
        log.error('Headers:%s, Format:%s\nData:%s', tabdata.headers, format_, each)
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
