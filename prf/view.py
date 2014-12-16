import json
import logging
import urllib
from collections import defaultdict
from pyramid.settings import asbool
from pyramid.request import Request
from pyramid.response import Response

from prf.json_httpexceptions import *
from prf.utils import dictset
from prf import wrappers

log = logging.getLogger(__name__)

ACTIONS = [
    'index',
    'show',
    'create',
    'update',
    'delete',
    'update_many',
    'delete_many',
    ]


class ViewMapper(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, view):
        # i.e index, create etc.
        action_name = self.kwargs['attr']

        def view_mapper_wrapper(context, request):
            matchdict = request.matchdict.copy()
            matchdict.pop('action', None)
            matchdict.pop('traverse', None)

            view_obj = view(context, request)
            action = getattr(view_obj, action_name)

            resp = action(**matchdict)

            if isinstance(resp, Response):
                return resp

            elif action_name in ['index', 'show']:
                resp = wrappers.wrap_in_dict(request, resp)
            elif action_name == 'create':
                resp = wrappers.wrap_in_http_created(request, resp)
            elif action_name in ['update', 'delete']:
                resp = wrappers.wrap_in_http_ok(request, resp)

            return resp

        return view_mapper_wrapper


class BaseView(object):

    __view_mapper__ = ViewMapper
    _default_renderer = 'json'

    def __init__(self, context, request, _params={}):
        self.context = context
        self.request = request
        self._model_class = None

        self._params = dictset(_params or request.params.mixed())
        ctype = request.content_type
        if request.method in ['POST', 'PUT', 'PATCH']:
            if ctype == 'application/json':
                try:
                    self._params.update(request.json)
                except ValueError, e:
                    log.error("Excpeting JSON. Received: '%s'. Request: %s %s"
                              , request.body, request.method, request.url)

        # no accept headers, use default
        if '' in request.accept:
            request.override_renderer = self._default_renderer

        elif 'application/json' in request.accept:
            request.override_renderer = 'prf_json'

        elif 'text/plain' in request.accept:
            request.override_renderer = 'string'

    def __getattr__(self, attr):
        if attr in ACTIONS:
            return self.not_allowed_action

        raise AttributeError(attr)

    def not_allowed_action(self, *a, **k):
        raise JHTTPMethodNotAllowed()

    def subrequest(self, url, params={}, method='GET'):
        req = Request.blank(url, cookies=self.request.cookies,
                            content_type='application/json', method=method)

        if req.method == 'GET' and params:
            req.body = urllib.urlencode(params)

        if req.method == 'POST':
            req.body = json.dumps(params)

        return self.request.invoke_subrequest(req)

    def needs_confirmation(self):
        return '__confirmation' not in self._params

    def delete_many(self, **kw):
        if not self._model_class:
            log.error('%s _model_class in invalid: %s',
                      self.__class__.__name__, self._model_class)
            raise JHTTPBadRequest

        objs = self._model_class.get_collection(**self._params)

        if self.needs_confirmation():
            return objs

        count = len(objs)
        objs.delete()
        return JHTTPOk('Deleted %s %s objects' % (count,
                       self._model_class.__name__))


class NoOp(BaseView):

    """Use this class as a stub if you want to layout all your resources before
    implementing actual views.
    """

    def index(self, **kw):
        return [dict(route=self.request.matched_route.name, kw=kw,
                params=self._params)]

    def show(self, **kw):
        return kw
