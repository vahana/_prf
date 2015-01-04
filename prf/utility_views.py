from pyramid.view import view_config

import prf
from prf.json_httpexceptions import *
from prf import wrappers
from prf.view import BaseView
from prf.utils import to_dicts, dictset

log = logging.getLogger(__name__)


class OptionsView(object):

    all_methods = set([
        'GET',
        'HEAD',
        'POST',
        'OPTIONS',
        'PUT',
        'DELETE',
        'PATCH',
        'TRACE',
        ])

    def __init__(self, request):
        self.request = request

    def __call__(self):
        request = self.request

        request.response.headers['Allow'] = ', '.join(self.all_methods)

        if 'Access-Control-Request-Method' in request.headers:

            request.response.headers['Access-Control-Allow-Methods'] = \
                ', '.join(self.all_methods)

        if 'Access-Control-Request-Headers' in request.headers:

            request.response.headers['Access-Control-Allow-Headers'] = \
                'origin, x-requested-with, content-type'

        return request.response


LOGNAME_MAP = dict(NOTSET=logging.NOTSET, DEBUG=logging.DEBUG,
                   INFO=logging.INFO, WARNING=logging.WARNING,
                   ERROR=logging.ERROR, CRITICAL=logging.CRITICAL)


class LogLevelsView(BaseView):

    def __init__(self, *arg, **kw):
        super(LogLevelsView, self).__init__(*arg, **kw)

        self.name = self.request.matchdict.get('id', 'root')
        if self.name == 'root':
            self.log = logging.getLogger()
        else:
            self.log = logging.getLogger(self.name)

    def setlevel(self, level):
        log.info("SET logger '%s' to '%s'" % (self.name, level))
        self.log.setLevel(LOGNAME_MAP[level])

    def show(self, id=None):
        return dict(logger=self.name,
                    level=logging.getLevelName(self.log.getEffectiveLevel()))

    def update(self, id=None):
        level = self._params['value'].upper()
        self.setlevel(level)

    def delete(self, id=None):
        self.setlevel('INFO')


class SettingsView(BaseView):

    settings = None
    __orig = None

    def __init__(self, *arg, **kw):
        super(SettingsView, self).__init__(*arg, **kw)

        SettingsView.settings = SettingsView.settings \
            or dictset(self.request.registry.settings)
        self.__orig = self.settings.copy()

    def index(self):
        return dict(self.settings)

    def show(self, id):
        return self.settings[id]

    def update(self, id):
        self.settings[id] = self._params['value']

    def create(self):
        key = self._params['key']
        value = self._params['value']

        self.settings[key] = value

    def delete(self, id):
        if 'reset' in self._params:
            self.settings[id] = self.request.registry.settings[id]
        else:
            self.settings.pop(id, None)

    def delete_many(self):
        if self.needs_confirmation():
            return self.settings.keys()

        for name, val in self.settings.items():
            self.settings[name] = self.__orig[name]

        return JHTTPOk('Reset the settings to original values')
