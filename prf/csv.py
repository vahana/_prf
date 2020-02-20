import logging
import os

from slovar import slovar

import prf
from prf.utils.utils import maybe_dotted, parse_specials, pager
from prf.utils.csv import (dict2tab, csv2dict, pd_read_csv,
                            get_csv_header, get_csv_total)

log = logging.getLogger(__name__)


class Results(list):
    def __init__(self, specials, data, total):
        list.__init__(self, [slovar(each) for each in data])
        self.total = total
        self.specials = specials


class CSV(object):

    def create_if(self, path):
        if os.path.isdir(path):
            return

        basedir = os.path.dirname(path)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        open(path, 'a').close()

    def __init__(self, ds, create=False, root_path=None):

        if ds.name.startswith('/'):
            file_name = ds.name
        else:
            root_path = prf.Settings.get('prf.csv.root', root_path)
            if not root_path:
                raise KeyError('Missing CSV backend `root_path` or `prf.csv.root` setting in config')

            file_name = os.path.join(root_path, ds.ns, ds.name)

        if not os.path.isfile(file_name):
            if create:
                self.create_if(file_name)
            else:
                log.error('File does not exist %s' % file_name)

        self.file_name = file_name
        self._total = None

    def get_file_or_buff(self):
        return self.file_name

    def clean_row(self, _dict):
        n_dict = slovar()

        def _n(text):
            text = text.strip()
            unders = ' ,\n'
            removes = '()/'

            clean = ''
            for ch in text:
                if ch in unders:
                    clean += '_'
                elif ch in removes:
                    pass
                else:
                    clean += ch

            return clean.lower()

        for kk,vv in list(_dict.items()):
            n_dict[_n(kk)] = vv

        return n_dict.unflat() # mongo freaks out when there are dots in the names

    def get_collection(self, **params):
        _, specials = parse_specials(slovar(params))

        if '_clean' in specials:
            processor = self.clean_row
        elif '_processor' in specials:
            processor = specials._processor
        else:
            processor = lambda x: x.unflat()

        if specials._count:
            return self.get_total(**specials)

        items = csv2dict(self.get_file_or_buff(), processor=processor,
                         fillna=specials.get('_fillna'),
                         **specials)

        return Results(specials, items, self.get_total(**specials))

    def get_collection_paged(self, page_size, **params):
        _, specials = parse_specials(slovar(params))

        #read the header here so get_collection doesnt need to
        params['_header'] = get_csv_header(self.get_file_or_buff())

        pgr = pager(specials._start, page_size, specials._limit)

        results = []
        for start, count in pgr():
            params.update({'_start':start, '_limit': count})
            results = self.get_collection(**params)
            yield results

    def get_total(self, **query):
        if not self._total:
            self._total = get_csv_total(self.get_file_or_buff())
        return self._total

    def drop_collection(self):
        try:
            os.remove(self.file_name)
        except FileNotFoundError as e:
            log.error(e)

    def unregister(self):
        pass
