import logging

from pandas.io import json
from slovar import slovar

from prf.utils import parse_specials
import pandas as pd

log = logging.getLogger(__name__)

def get_csv_header(file_or_buff):
    return pd.read_csv(file_or_buff, nrows=0, engine = 'c').columns.to_list()


def get_csv_total(file_or_buff):
    df = pd.read_csv(file_or_buff, header=[0], engine = 'c')
    return df.shape[0]


def get_json_total(file_or_buff):
    return len(json2dict(file_or_buff))


def pd_read_csv(file_or_buff, **params):
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


def df2dict(df, **kw):
    def pop_na(row):
        _d = slovar(row.to_dict())
        if kw.get('_fillna') is None:
            return _d.pop_by_values([''])
        else:
            return _d

    processor = kw.get('_processor') or (lambda x: x)
    results = []

    for chunk in df:
        for each in chunk.fillna(kw.get('_fillna', '')).iterrows():
            results.append(processor(pop_na(each[1])))

    return results


def csv2dict(file_or_buff, **kw):
    results = []

    for chunk in pd_read_csv(file_or_buff, **kw):
        for each in chunk.fillna(kw.get('_fillna', '')).iterrows():
            results.append(each[1])

    return results

import json
def json2dict(file_or_buff, **kw):
    specials = slovar(kw)
    results = []
    with open(file_or_buff) as _f:
        results = json.load(_f)

    if not isinstance(results, (list)):
        return [results]

    if kw:
        return results[specials._start:specials._limit]

    return results
