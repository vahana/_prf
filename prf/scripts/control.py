import sys
import subprocess

from prf.utils import dictset
from pyramid.paster import get_appsettings, setup_logging
from pyramid.scripts.common import parse_vars


def package_name(argv):
    return argv[0].split('/')[-1].split('.')[0]


def pid_arg(package_name):
    return '--pid-file=%s.pid' % package_name


def config_uri(package_name):
    return '%s.ini' % package_name


def call_pserve(argv):
    argv.insert(0, 'pserve')
    return subprocess.call(argv)

def start(argv=sys.argv):
    pname = package_name(argv)
    options = parse_vars(argv[1:])
    config = config_uri(pname)

    setup_logging(config)
    settings = dictset(get_appsettings(config, pname, options=options))

    pargs = [config]
    if settings.asbool('daemonize', False):
        pargs += [pid_arg(pname), 'start']

    return call_pserve(pargs + argv[1:])


def stop(argv=sys.argv):
    pname = package_name(argv)
    return call_pserve([config_uri(pname), pid_arg(pname), 'stop'])


def status(argv=sys.argv):
    pname = package_name(argv)
    return call_pserve([config_uri(pname), pid_arg(pname), 'status'])
