import logging
from pyramid.view import view_config
from pyramid.security import remember, forget, NO_PERMISSION_REQUIRED

from slovar import slovar

import prf
from prf.view import BaseView

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
            or slovar(self.request.registry.settings)
        self.__orig = self.settings.copy()
        self._params.setdefault('_flat', 1)

    def index(self):
        return slovar(self.settings).extract(self._params.get('_fields'))

    def show(self, id):
        return self.settings.extract(id)

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
            return list(self.settings.keys())

        for name, val in list(self.settings.items()):
            self.settings[name] = self.__orig[name]


class APIView(BaseView):

    def _get_routes(self):
        root = list(self.request.registry['prf.root_resources'].values())[0]
        mapper = root.config.get_routes_mapper()
        return mapper.routes

    def show(self):
        return {'api': sorted(['%s'% (r.path)
                    for r in list(self._get_routes().values())])}


class AccountView(BaseView):

    _user_model = None

    @classmethod
    def set_user_model(cls, model):

        def check_callable(model, name):
            if not getattr(model, name, None):
                raise AttributeError("%s model must have '%s' class method"
                                     % (model, name))

        check_callable(model, 'authenticate')

        cls._user_model = model

    def login(self):
        login = self._params.asstr('login')
        password = self._params.asstr('password')
        next = self._params.get('next', '')

        if '@' in login:
            user = self._user_model.objects(email=login).first()
        else:
            user = self._user_model.objects(username=login).first()

        if not user:
            raise prf.exc.HTTPUnauthorized("Unknown user '%s'" % login)

        if user.is_expired():
            user.status = 'expired'
            raise prf.exc.HTTPUnauthorized("Account expired for '%s'" % login)

        success = user.authenticate(password)

        if success:
            headers = remember(self.request, str(user.id))
            if next:
                return prf.exc.HTTPFound(headers=headers, location=next)
            return prf.exc.HTTPOk(headers=headers)
        else:
            raise prf.exc.HTTPUnauthorized("User '%s' failed to Login" % login)

    def logout(self):
        next = self.request.params.get('next', '')

        headers = forget(self.request)

        if next:
            return prf.exc.HTTPFound(headers=headers, location=next)

        return prf.exc.HTTPOk(headers=headers)


from prf.mongodb import get_document_cls
from prf.auth import BaseACL

class MongoACL(BaseACL):
    def get_item(self, key):
        return get_document_cls(key)

class MongoView(BaseView):
    _acl = MongoACL

    def show(self, id):
        self.returns_many = True
        objs = get_document_cls(id).get_collection(**self._params)
        return objs
