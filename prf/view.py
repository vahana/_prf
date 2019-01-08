import re
import json
import logging
import urllib.request, urllib.parse, urllib.error
from datetime import datetime
import uuid

from pyramid.request import Request
from pyramid.response import Response

from slovar import slovar

import prf.exc
from prf.utils import json_dumps, urlencode
from prf.serializer import DynamicSchema
from prf import resource
from prf.utils import process_fields, typecast, Params, parse_specials

log = logging.getLogger(__name__)


CONSTANTS = {
    '_TODAY': lambda: datetime.utcnow().strftime('%Y_%m_%d'),
    '_NOW': lambda: datetime.utcnow().strftime('%Y_%m_%dT%H_%M_%S'),
    '_UUID': lambda: uuid.uuid4().get_hex()
}

MAX_NB_PARAMS = 512
MAX_QS_LENGTH = 8000


class ViewMapper(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, view):
        action_name = self.kwargs['attr']

        def view_mapper_wrapper(context, request):
            matchdict = request.matchdict.copy()
            matchdict.pop('action', None)
            matchdict.pop('traverse', None)

            view_obj = view(context, request)
            action = getattr(view_obj, action_name)
            ret = action(**matchdict)

            return ret

        return view_mapper_wrapper


class BaseView(object):
    """Base class for inheriting the views.

    Attributes:
        _params (dict): stores parsed query string if its GET
            or request json body if its POST or PUT. Standard values supported:
            `_limit` (int): number of resources to return
            `_start` (int): offset
            `_page` (int): page number starting with 0, with _limit
            `_sort` (str): comma separated list of fields to sort by (default DESC)
            `_fields` (str): resource projection, list of fields to return
                            (or exclude if name of the field starts with `-`)
            `_count` (bool): return the count

            Example: GET /users?_limit=10&start=100&_sort=created_at&_fields=id,email,created_at

        request (Request): request object passed to the view by pyramid

    Example:
        class UsersView(BaseView):
            def index(self):
                return list_of_users

            def show(self, id):
                return get_user(user_id=id)

            def create(self):
                user = create_user(self._params)
                return user

            def update(self, id):
                update_user(user_id, user_attr=self._params)

            def delete(self, id):
                delete_user(user_id=id)

    """

    __view_mapper__ = ViewMapper
    _default_renderer = 'json'
    _serializer = DynamicSchema
    _acl = None
    _model = None
    _conf_keyword = '__CONFIRMATION'
    _default_params = {
        '_limit': 20
    }

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self._model_class = None
        self.returns_many = False
        self.post_as_get = False

        self.raise_not_found = self.get_settings(request).\
                                    asbool('prf.raise_not_found',
                                            default=True)

        self.__params, self._specials = self.process_params(request)

        # self.process_variables()
        self.set_renderer()
        self.init()

    def init(self):
        pass
        #to override in children

    @property
    def resource(self):
        rname = self.request.matched_route.name
        return self.request.resource_map.get(rname)

    @property
    def _params(self):
        return self.__params

    @_params.setter
    def _params(self, val):
        self.__params = Params(val)

    def set_renderer(self):
        # no accept headers, use default
        if '' in self.request.accept:
            self.request.override_renderer = self._default_renderer

        elif 'application/json' in self.request.accept:
            self.request.override_renderer = 'json'

        elif 'text/plain' in self.request.accept:
            self.request.override_renderer = 'string'

        elif ('text/csv' in self.request.accept or
             'text/xls' in self.request.accept):
            self.request.override_renderer = 'tab'

    def process_params(self, request):
        ctype = request.content_type

        _params = Params(request.params.mixed())

        if 'application/json' in ctype:
            if request.method in ['POST', 'PUT', 'PATCH']:
                try:
                    _params.update(request.json)
                except ValueError as e:
                    raise prf.exc.HTTPBadRequest(
                        "Expecting JSON. Received: '%s'. Request: %s %s"
                            % (request.body, request.method, request.url))

        _params = Params(typecast(_params))

        if request.method == 'GET':
            settings = self.get_settings(request)

            param_limit = settings.asint('prf.request.max_params', default=MAX_NB_PARAMS)
            qs_limit = settings.asint('prf.request.max_qs_length', default=MAX_QS_LENGTH)

            if len(_params) > param_limit:
                raise prf.exc.HTTPRequestURITooLong('Max limit of params is %s. Got %s' %
                                    (param_limit, len(_params)))

            if len(request.query_string) > qs_limit:
                raise prf.exc.HTTPRequestURITooLong('Max query string length is %s characters. Got %s' %
                                    (qs_limit, len(request.query_string)))

            _params = _params.merge_with(self._default_params)

        _, _specials = parse_specials(_params.copy())
        return _params, _specials

    def process_variables(self):
        if self.request.method  == 'GET':
            return

        flat_params = self._params.flat()

        def process_each(param):
            name_parts = []
            for each in re.split('\$([^$]*)\$', param):
                if each in CONSTANTS:
                    name_parts.append(CONSTANTS[each]())
                elif each in flat_params:
                    val = process_each(flat_params[each])
                    name_parts.append(val)
                else:
                    name_parts.append(each)

            if name_parts:
                param = ''.join(name_parts)

            return param

        for key, val in list(flat_params.items()):
            if not isinstance(val, str):
                continue

            flat_params[key] = process_each(val)

        self._params = flat_params.unflat()

    def __getattr__(self, attr):
        if attr in resource.Actions.all():
            return self.not_allowed_action

        raise AttributeError(attr)

    def serialize(self, obj, many):
        def get_meta(meta, total):
            if meta:
                return meta
            return slovar(total = total)

        def process_dict(_d):
            _d = slovar(_d)
            if self._specials._pop_empty:
                _d = _d.pop_by_values([[], {}, ''])

            if self._specials._flat:
                if '*' in self._specials._flat:
                    _d = _d.flat()
                else:
                    _d = _d.flat(self._specials._flat)

            return _d.extract(self._specials._fields)

        _total = getattr(obj, 'total', None)
        _meta = getattr(obj, '_meta', None)

        if many and 'data' in obj:
            if 'total' in obj:
                _total = obj['total']
            else:
                _total = len(obj['data'])
            obj = obj['data']

        if isinstance(obj, dict):
            _d = process_dict(obj)
            return _d, _total or len(_d), _meta

        elif isinstance(obj, list):
            data = []
            for each in obj:
                if isinstance(each, dict):
                    each = process_dict(each)
                elif hasattr(each, 'to_dict'):
                    each = process_dict(each.to_dict())
                data.append(each)

            return data, _total or len(data), _meta

        else:
            if many:
                if hasattr(obj, '_total'):
                    _total = obj._total
                data = [process_dict(each.to_dict()) for each in obj]
            else:
                data = process_dict(obj.to_dict())

            return data, _total or len(data), _meta


    def _process(self, data, many):
        def wrap2dict(data, total, meta=None):
            wrapper = {
                'total': total,
                'count': len(data),
                'query': self._params,
            }

            if meta:
                wrapper['_meta'] = meta

            wrapper['data'] = data
            return wrapper

        if self._specials._count:
            return data

        if not data:
            return wrap2dict([], 0)

        serialized, _total, _meta = self.serialize(data, many=many)
        return wrap2dict(self.add_meta(serialized), _total, _meta)


    def _index(self, **kw):
        return self._process(self.index(**kw), many=True)

    def _show(self, **kw):
        data = self._process(self.show(**kw), many=self.returns_many)
        if not data:
            if not self.returns_many:
                if self.raise_not_found:
                    raise prf.exc.HTTPNotFound(
                        "'%s' resource not found" % (self.request.path))
                return data
            else:
                return {}

        return data['data'] if self.returns_many == False else data

    def _create(self, **kw):
        obj = self.create(**kw)

        if self.post_as_get:
            return self._process(obj, many=self.returns_many)

        if not obj:
            return prf.exc.HTTPCreated()
        elif isinstance(obj, Response):
            return obj
        else:
            return prf.exc.HTTPCreated(
                        location=self.request.current_route_url(obj.id),
                        resource=self.serialize(obj, many=False)[0])

    def _update(self, **kw):
        return self.update(**kw) or prf.exc.HTTPOk()

    def _patch(self, **kw):
        return self.patch(**kw) or prf.exc.HTTPOk()

    def _delete(self, **kw):
        return self.delete(**kw) or prf.exc.HTTPOk()

    def _update_many(self, **kw):
        return self.update_many(**kw) or prf.exc.HTTPOk()

    def _delete_many(self, **kw):
        return self.delete_many(**kw) or prf.exc.HTTPOk()

    def not_allowed_action(self, *a, **k):
        raise prf.exc.HTTPMethodNotAllowed()

    def subrequest(self, url, params={}, method='GET'):
        req = Request.blank(url, cookies=self.request.cookies,
                            content_type='application/json', method=method)

        if req.method == 'GET' and params:
            req.text = urlencode(params)

        if req.method == 'POST':
            req.text = json_dumps(params)

        return self.request.invoke_subrequest(req)

    def needs_confirmation(self):
        if self._conf_keyword in self._params:
            self._params.pop(self._conf_keyword)
            return False

        return True

    def delete_many(self, **kw):
        if not self._model_class:
            log.error('%s _model_class in invalid: %s',
                      self.__class__.__name__, self._model_class)
            raise prf.exc.HTTPBadRequest()

        objs = self._model_class.get_collection(**self._params)

        if self.needs_confirmation():
            return objs

        count = len(objs)
        objs.delete()
        return prf.exc.HTTPOk('Deleted %s %s objects' % (count,
                       self._model_class.__name__))

    def add_meta(self, collection):
        try:
            for each in collection:
                try:
                    url = urllib.parse.urlparse(self.request.current_route_url())._replace(query='')
                    id_name = self.resource.id_name
                    val = urllib.parse.quote(str(each[id_name]))

                    if self.returns_many == True: # show action returned a collection
                        _id = '?%s=%s' % (id_name, val)
                    else:
                        _id = '/%s' % val

                    each.setdefault('self', '%s%s' % (url.geturl(), _id))

                except TypeError:
                    pass
        except (TypeError, KeyError):
            pass
        finally:
            return collection

    def get_settings(self, request):
        return Params(request.registry.settings)

class NoOp(BaseView):

    """Use this class as a stub if you want to layout all your resources before
    implementing actual views.
    """

    def index(self, **kw):
        return [dict(route=self.request.matched_route.name, kw=kw,
                params=self._params)]

    def show(self, **kw):
        return kw
