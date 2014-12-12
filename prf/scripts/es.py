from argparse import ArgumentParser
import sys
import textwrap
import urlparse
import logging
from pyramid.paster import bootstrap

from prf.elasticsearch import ES
from prf.utils import dictset, get_document_cls, split_strip


def main(argv=sys.argv, quiet=False):
    log = logging.getLogger()
    log.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

    command = ESCommand(argv, log)
    return command.run()


class ESCommand(object):

    bootstrap = (bootstrap,)
    stdout = sys.stdout
    usage = '%prog config_uri <models'

    def __init__(self, argv, log):
        parser = ArgumentParser(description=__doc__)

        parser.add_argument(
            '-c', '--config', help='config.ini (required)', required=True)
        parser.add_argument(
            '--quiet', help='quiet mode', action='store_true', default=False)
        parser.add_argument(
            '--models', help='explicit list of models to index', required=True)
        parser.add_argument(
            '--params', help='url encoded params for each model')
        parser.add_argument('--index', help='index name', default=None)

        self.options = parser.parse_args()
        if not self.options.config:
            return parser.print_help()

        env = self.bootstrap[0](self.options.config)
        registry = env['registry']

        self.log = log

        if not self.options.quiet:
            self.log.setLevel(logging.INFO)

        self.settings = dictset(registry.settings)

    def run(self, quiet=False):
        ES.setup(self.settings, index_name=self.options.index)
        models = split_strip(self.options.models)

        for model in models:
            params = self.options.params or ''
            params = dict([[k, v[0]]
                           for k, v in urlparse.parse_qs(params).items()])
            ES(model).index(params)

        return 0
