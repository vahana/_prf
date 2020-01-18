import os
import sys
import logging

from argparse import ArgumentParser
import pymongo

from urllib.parse import urlparse
from prf.utils import qs2dict

log = logging
# log.basicConfig(level=logging.DEBUG)


class Script(object):
    def __init__(self, argv):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument('--host', default='localhost')
        parser.add_argument('--port', default=27017)
        parser.add_argument('--dry', action='store_true')

        parser.add_argument('uri')
        parser.add_argument('-d', '--db')
        parser.add_argument('-c', '--collection')
        parser.add_argument('-i', '--index', action='append', help='Index name in the following format <name>:sparse:background')

        self.args = parser.parse_args()
        self.parser = parser

        if '/' not in self.args.uri:
            self.parser.error('provide input as `db/collection/indexes, where indexes are comma separated <name>:sparse:background`')

        parts = urlparse(self.args.uri)
        path_parts = parts.path.split('/')
        path_index = None

        if len(path_parts) == 2:
            self.db, self.collection = path_parts
        elif len(path_parts) == 3:
            self.db, self.collection,path_index = path_parts
        else:
            return self.parser.error('Bad path, must be either 2 or 3 parts')

        self.index = qs2dict(parts.query)
        if path_index:
            self.index['index'] = path_index

    def run(self):
        con = pymongo.MongoClient(host=self.args.host, port=self.args.port)
        db = con.get_database(self.db)
        col = db.get_collection(self.collection)

        indexes = []
        for op in self.index:
            _inds = self.index.aslist(op)

            params = {}
            if op != 'index':
                params[op] = True
                indexes.append(pymongo.IndexModel([(it, pymongo.DESCENDING) for it in _inds], **params))
            else:
                for ind in _inds:
                    indexes.append(pymongo.IndexModel(ind, **params))

        if self.args.dry:
            print('>>> DRY RUN <<<')

        print('Creating indexes on %s.%s:' % (self.db, self.collection))

        for ind in indexes:
            print(ind.document)

        if not self.args.dry:
            col.create_indexes(indexes)

def run():
    Script(sys.argv).run()
