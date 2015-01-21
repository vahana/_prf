import os
import sys
import logging

from sqlalchemy import engine_from_config
from sqlalchemy_utils.functions import database_exists, create_database,\
                                       drop_database
from sqlalchemy.orm import scoped_session, sessionmaker

from pyramid.paster import get_appsettings, setup_logging
from pyramid.scripts.common import parse_vars

from prf.utils import dictset
from prf.scripts.common import package_name, pid_arg, config_uri

log = logging.getLogger(__name__)

def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri> [--drop]\n'
          '(example: "%s development.ini --drop")' % (cmd, cmd))
    sys.exit(1)


def main(argv=sys.argv):
    if len(argv) < 2:
        usage(argv)

    pname = package_name(argv)
    config = argv[1]

    if len(argv) > 2 and '--drop' == argv[2]:
        drop = True
    else:
        drop = False

    # options = parse_vars(argv[2:])
    setup_logging(config)
    settings = dictset(get_appsettings(config, pname))

    engine = engine_from_config(settings, 'sqlalchemy.')

    if drop and database_exists(engine.url):
        in_ = raw_input('Are you sure you want to drop?(y/n): ')
        if in_ == 'y':
            drop_database(engine.url)
            create_database(engine.url)
        else:
            print 'Canceled'
            return

    module = __import__(pname)
    base = module.model.configure_session(settings)
    base.metadata.create_all()
    print('DB initialized' + (' (new)' if drop else ''))