import re
import json
import logging
import urllib
from datetime import datetime
import uuid

from urlparse import urlparse
from pyramid.request import Request
from pyramid.response import Response

import prf.exc
from prf.utils import dictset, issequence, json_dumps, urlencode
from prf.serializer import DynamicSchema
from prf import resource
from prf.utils import process_fields, dkdict
from prf.utils.qs import typecast

log = logging.getLogger(__name__)


CONSTANTS = {
    '_TODAY': lambda: datetime.utcnow().strftime('%Y_%m_%d'),
    '_NOW': lambda: datetime.utcnow().strftime('%Y_%m_%dT%H_%M_%S'),
    '_UUID': lambda: uuid.uuid4().get_hex()
}


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
    _id_name = None
    _model = None

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self._model_class = None
        self.returns_many = False
        self.post_as_get = False

        self._params = self.process_params(request)
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

    @classmethod
    def process_params(cls, request):
        ctype = request.content_type

        _params = dictset(request.params.mixed())
        if 'application/json' in ctype:
            if request.method in ['POST', 'PUT', 'PATCH', 'DELETE']:
                try:
                    _params.update(request.json)
                except ValueError, e:
                    raise prf.exc.HTTPBadRequest(
                        "Expecting JSON. Received: '%s'. Request: %s %s"
                            % (request.body, request.method, request.url))

        _params = dkdict(typecast(_params))
        return _params

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

        for key, val in flat_params.items():
            if not isinstance(val, basestring):
                continue

            flat_params[key] = process_each(val)

        self._params = flat_params.unflat()

    def __getattr__(self, attr):
        if attr in resource.Actions.all():
            return self.not_allowed_action

        raise AttributeError(attr)

    def serialize(self, obj, many):
        fields = self._params.get('_fields')
        flat = self._params.asbool('_flat', default=False)
        pop_empty = self._params.asbool('_pop_empty', default=False)

        serializer = self._serializer(
                            context={'request':self.request, 'fields':fields, 'flat': flat, 'pop_empty': pop_empty},
                            many=many, strict=True,
                            **process_fields(fields).subset('only,exclude'))

        return serializer.dump(obj).data

    def process_builtins(self, obj, many):
        if not isinstance(obj, (list, dict)):
            return obj, len(obj)

        fields = self._params.get('_fields')

        def process_dict(d_):
            d_ = dictset(d_).extract(fields)
            if self._params.asbool('_pop_empty', default=False):
                d_ = d_.flat(keep_lists=True).pop_by_values([[], {}, '']).unflat()
            return d_

        _total = None

        if many and 'data' in obj:
            if 'total' in obj:
                _total = obj['total']
            else:
                _total = len(obj['data'])
            obj = obj['data']

        if isinstance(obj, dict):
            _d = process_dict(obj)
            return _d, _total or len(_d)

        elif isinstance(obj, list):
            data = []
            for each in obj:
                if isinstance(each, dict):
                    each = process_dict(each)
                elif hasattr(each, 'to_dict'):
                    each = each.to_dict(fields)

                data.append(each)
            obj = data
            return obj, _total or len(obj)

    def _process(self, data, many):
        if '_count' in self._params:
            return data

        if isinstance(data, (list, dict)):
            serialized, _total = self.process_builtins(data, many=many)
        else:
            serialized = self.serialize(data, many=many)
            _total = getattr(data, '_total', len(serialized))

        return dictset({
            'total': _total,
            'count': len(serialized),
            'data': self.add_meta(serialized),
            'query': self._params,
        })

    def _index(self, **kw):
        return self._process(self.index(**kw), many=True)

    def _show(self, **kw):
        data = self._process(self.show(**kw), many=self.returns_many)
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
                        resource=self.serialize(obj, many=False))

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
            req.body = urlencode(params)

        if req.method == 'POST':
            req.body = json_dumps(params)

        return self.request.invoke_subrequest(req)

    def needs_confirmation(self):
        return self._params.pop('__CONFIRMATION', True)

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
                    url = urlparse(self.request.current_route_url())._replace(query='')
                    id_name = self._id_name or 'id'
                    val = urllib.quote(str(each[id_name]))

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

    def get_settings(self, key=None):
        if key:
            return self.request.registry.settings[key]
        return dictset(self.request.registry.settings)

class NoOp(BaseView):

    """Use this class as a stub if you want to layout all your resources before
    implementing actual views.
    """

    def index(self, **kw):
        return [dict(route=self.request.matched_route.name, kw=kw,
                params=self._params)]

    def show(self, **kw):
        return kw
