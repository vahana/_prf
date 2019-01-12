import os
import sys
import logging
from pprint import pprint as pp
from datetime import datetime

from prf.request import Request
from prf.utils.utils import TODAY, split_strip

from argparse import ArgumentParser

log = logging
# log.basicConfig(level=logging.DEBUG)

class Script(object):
    def __init__(self, argv):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument('--host', default='localhost')
        parser.add_argument('--port', default=9200)
        parser.add_argument('-r', '--repo', default='es2.s3')
        parser.add_argument('-s', '--snapname')
        parser.add_argument('-i', '--indices')
        parser.add_argument('--rename_to')

        parser.add_argument('--silent', action='store_true')
        parser.add_argument('--ls', action='store_true')
        parser.add_argument('--snapshot', action='store_true')
        parser.add_argument('--restore', action='store_true')
        parser.add_argument('--current', action='store_true')

        self.parser = parser
        self.args = parser.parse_args()
        self.api = Request('http://%s:%s' % (self.args.host, self.args.port))

        self.silent = self.args.silent or (self.args.ls or self.args.current)

    def request(self, method, **kw):
        print('URL:', self.api.base_url)
        pp(kw)

        if not self.silent:
            if input('Hit ENTER to run  ') == '':
                pp(getattr(self.api, method)(**kw).json())
            else:
                print('Canceled')
        else:
            pp(getattr(self.api, method)(**kw).json())

    def get_snapname(self, name):
        now = datetime.utcnow()
        seconds_since_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0))\
                                     .total_seconds())
        return '%s_%s_%s' % (TODAY(), name, seconds_since_midnight)

    def index_exists(self, index):
        return self.api.head(index).ok

    def snapshot(self):
        snapname = self.args.snapname
        indices = split_strip(self.args.indices)

        for each in indices:
            if not self.index_exists(each):
                log.warning('index `%s` does not exist', each)

        if snapname and '*' in snapname:
            snapname = self.get_snapname(snapname.replace('*', ''))
        elif not snapname:
            if len(indices)>1:
                log.warning('more than 1 index specified. forgot -s arg ?')
            snapname = self.get_snapname(indices[0])

        params = dict(
            path = '_snapshot/%s/%s' % (self.args.repo, snapname),
            data = dict(
                indices=self.args.indices
            )
        )
        self.request('post', **params)

    def restore(self):
        params = dict(
            path = '_snapshot/%s/%s/_restore' % (self.args.repo, self.args.snapname),
            data = dict(
                indices=self.args.indices,
                rename_pattern=self.args.indices,
                rename_replacement=self.args.rename_to or '%s_%s' % (self.args.snapname, self.args.indices),
                include_aliases='false'
            )
        )
        self.request('post', **params)

    def ls(self, index='_all'):
        params = dict(
            path = '_snapshot/%s/%s' % (self.args.repo, index)
        )
        self.request('get', **params)

    def current(self):
        params = dict(
            path = '_snapshot/%s/_current' % self.args.repo
        )
        self.request('get', **params)

    def run(self):
        if self.args.ls:
            self.ls()
        elif self.args.current:
            self.current()
        elif self.args.snapshot:
            self.snapshot()
        elif self.args.restore:
            self.restore()
        else:
            self.parser.error('Nothing to do')

def run():
    Script(sys.argv).run()
