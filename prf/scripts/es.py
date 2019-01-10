import os
import sys
import logging
from pprint import pprint as pp
from datetime import datetime

from prf.request import Request
from prf.utils.utils import TODAY

from argparse import ArgumentParser

log = logging
log.basicConfig(level=logging.DEBUG)

ACTIONS = ['ls', 'snapshot', 'restore', 'current', ]

class Script(object):
    def __init__(self, argv):
        parser = ArgumentParser(description=__doc__)
        parser.add_argument('--host', default='localhost')
        parser.add_argument('--port', default=9200)
        parser.add_argument('-r', '--repo', default='es2.s3')
        parser.add_argument('-s', '--snapname')
        parser.add_argument('-i', '--indices')
        parser.add_argument('--rename_to')
        parser.add_argument('-a', '--action', required=True, choices=ACTIONS)
        parser.add_argument('--silent', action='store_true')

        self.args = parser.parse_args()
        self.api = Request('http://%s:%s' % (self.args.host, self.args.port))

        if (self.args.action in ['snapshot', 'restore']) and not (self.args.snapname or self.args.indices):
            parser.error('snapname and/or indices missing')

        self.silent = self.args.silent or self.args.action in ['ls', 'current']

    def js(self, resp):
        pp(resp.json())

    def request(self, method, **kw):
        print('URL:', self.api.base_url)
        pp(kw)

        if not self.silent:
            if input('Hit ENTER to run  ').upper() == '':
                self.js(getattr(self.api, method)(**kw))
            else:
                print('Canceled')
        else:
            self.js(getattr(self.api, method)(**kw))

    def get_snapname(self, name):
        now = datetime.utcnow()
        seconds_since_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0))\
                                     .total_seconds())
        return '%s_%s_%s' % (TODAY(), name, seconds_since_midnight)

    def snapshot(self):
        snapname = self.args.snapname or self.get_snapname(self.args.indices)

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
        getattr(self, self.args.action)()

def run():
    Script(sys.argv).run()
