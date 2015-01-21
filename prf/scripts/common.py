def package_name(argv):
    return argv[0].split('/')[-1].split('.')[0]


def pid_arg(package_name):
    return '--pid-file=%s.pid' % package_name


def config_uri(package_name):
    return '%s.ini' % package_name
