import os
import sys
import logging

from argparse import ArgumentParser
from pymongo import MongoClient
from pymongo import IndexModel

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

        parts = self.args.uri.split('/')
        if len(parts) != 3:
            self.parser.error('uri must have 3 parts `db/collection/index`')
        self.db, self.collection, self.index = parts
        self.index = self.index.split(',')

    def run(self):
        con = MongoClient(host=self.args.host, port=self.args.port)
        db = con.get_database(self.db)
        col = db.get_collection(self.collection)

        indexes = []
        for each in self.index:
            parts = each.split(':')
            key = parts[0]

            if not key:
                return self.parser.error('missing index name')

            params = {}

            if 'sparse' in parts:
                params['sparse'] = True
            if 'no_background' in parts:
                params['background'] = False
            if 'unique' in parts:
                params['unique'] = True
            else:
                params['background'] = True

            indexes.append(IndexModel(key, **params))

        if self.args.dry:
            print('>>> DRY RUN <<<')

        print('Creating indexes on %s.%s:' % (self.db, self.collection))

        for ind in indexes:
            print(ind.document)

        if not self.args.dry:
            col.create_indexes(indexes)

def run():
    Script(sys.argv).run()
