import logging
import os

from slovar import slovar

from prf.utils.utils import parse_specials, pager
from prf.utils import (
    csv2dict, get_csv_total, json2dict, get_json_total)

log = logging.getLogger(__name__)


def includeme(config):
    Settings = slovar(config.registry.settings)
    FS.setup(Settings)


class FileReader:
    @staticmethod
    def get_format_from_file(file_or_buff):
        format = ''

        if isinstance(file_or_buff, str):
            _, format = os.path.splitext(file_or_buff)
        else:
            raise ValueError('Failed to get format from file')

        if format.startswith('.'):
            format = format[1:]

        return format.lower()

    def __init__(self, file_or_buff, format=None):
        self.format = format or FileReader.get_format_from_file(file_or_buff)
        self.file_or_buff = file_or_buff
        self.total = None

    def to_dicts(self, **kw):

        #make sure if its a file object, its reset to 0
        if hasattr(self.file_or_buff, 'seekable'):
            self.file_or_buff.seek(0)

        if self.format == 'csv':
            return csv2dict(self.file_or_buff, **kw)

        if self.format == 'json':
            return json2dict(self.file_or_buff, **kw)

    def get_total(self, **kw):

        if self.total:
            return self.total

        if self.format == 'csv':
            self.total = get_csv_total(self.file_or_buff)

        elif self.format == 'json':
            self.total = get_json_total(self.file_or_buff)

        return self.total


class Results(list):
    def __init__(self, specials, data, total):
        list.__init__(self, [slovar(each) for each in data])
        self.total = total
        self.specials = specials


class FS(object):

    @classmethod
    def setup(cls, settings):
        cls.settings = settings.copy()

    def __init__(self, ds, create=False, root_path=None, reader=None):

        if ds.name.startswith('/'):
            file_name = ds.name
        else:
            if not root_path:
                raise KeyError('Missing `root_path`')

            file_name = os.path.join(root_path, ds.ns, ds.name)

        if not os.path.isfile(file_name):
            if create:
                self.create_if(file_name)
            else:
                log.error('File does not exist %s' % file_name)

        self.reader = reader or FileReader(file_name)
        self.file_name = file_name
        self._total = None

    def get_file_or_buff(self):
        return self.file_name

    def create_if(self, path):
        if os.path.isdir(path):
            return

        basedir = os.path.dirname(path)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        open(path, 'a').close()

    def get_collection(self, **params):
        _, specials = parse_specials(slovar(params))

        if specials._count:
            return self.get_total(**specials)

        items = self.reader.to_dicts(**specials)

        return Results(specials, items, self.get_total(**specials))

    def get_collection_paged(self, page_size, **params):
        _, specials = parse_specials(slovar(params))
        pgr = pager(specials._start, page_size, specials._limit)

        results = []
        for start, count in pgr():
            params.update({'_start':start, '_limit': count})
            results = self.get_collection(**params)
            yield results

    def get_total(self, **query):
        if not self._total:
            self._total = self.reader.get_total()
        return self._total

    def drop_collection(self):
        try:
            os.remove(self.file_name)
        except FileNotFoundError as e:
            log.error(e)

    def unregister(self):
        pass
