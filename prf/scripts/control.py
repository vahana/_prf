import sys
import subprocess

from pyramid.paster import get_appsettings, setup_logging
from pyramid.scripts.common import parse_vars

from slovar import slovar
from prf.scripts.common import package_name, pid_arg, config_uri


def call_pserve(argv):
    argv.insert(0, 'pserve')
    return subprocess.call(argv)

def call_pshell(argv):
    argv.insert(0, 'pshell')
    return subprocess.call(argv)

def start(argv=sys.argv):
    pname = package_name(argv)
    try:
        config = argv[1]
    except IndexError:
        raise ValueError('No config file')

    options = parse_vars(argv[2:])

    setup_logging(config)
    settings = slovar(get_appsettings(config, pname, options=options))

    pargs = [config]
    if settings.asbool('daemonize', False):
        pargs += [pid_arg(pname), 'start']

    return call_pserve(pargs + argv[2:])

def shell(argv=sys.argv):
    pname = package_name(argv)
    config = argv[1]
    options = parse_vars(argv[2:])

    setup_logging(config)
    settings = slovar(get_appsettings(config, pname, options=options))
    return call_pshell([config] + argv[2:])


def stop(argv=sys.argv):
    pname = package_name(argv)
    return call_pserve([config_uri(pname), pid_arg(pname), 'stop'])


def status(argv=sys.argv):
    pname = package_name(argv)
    return call_pserve([config_uri(pname), pid_arg(pname), 'status'])
