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

ACTIONS = ['index', 'show', 'create', 'update', 'delete', 'update_many', 'delete_many']

class ViewMapper(object):
    "mapper class for BaseView"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, view):
        #i.e index, create etc.
        action_name = self.kwargs['attr']

        def view_mapper_wrapper(context, request):
            matchdict = request.matchdict.copy()
            matchdict.pop('action', None)
            matchdict.pop('traverse', None)

            #instance of BaseView (or child of)
            view_obj = view(context, request)
            action = getattr(view_obj, action_name)
            request.action = action_name

            try:
                # run before_calls (validators) before running the action
                for call in view_obj._before_calls.get(action_name, []):
                    call(request=request)

            except wrappers.ValidationError, e:
                log.error('validation error: %s', e)
                raise JHTTPBadRequest(e.args)

            request._after_calls = view_obj._after_calls
            resp = action(**matchdict)

            # if its Response object (or HTTPErrors that are derived from Response)
            # then dont call after calls
            if isinstance(resp, Response):
                return resp

            for call in view_obj._after_calls.get(request.action, []):
                if request.action in ['index', 'show']:
                    resp = call(**dict(request=request, result=resp))

            return resp

        return view_mapper_wrapper


class BaseView(object):
    """Base class for prf views.
    """
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
                except ValueError as e:
                    log.error("Excpeting JSON. Received: '%s'. Request: %s %s",
                                        request.body, request.method, request.url)


        # dict of the callables {action_name:[callable1, callable2..]}
        # before calls are executed before the action is called
        # after_calls are called after the action returns.
        self._before_calls = defaultdict(list)
        self._after_calls = defaultdict(list)

        # no accept headers, use default
        if '' in request.accept:
            request.override_renderer = self._default_renderer

        elif 'application/json' in request.accept:
            request.override_renderer = 'prf_json'

        elif 'text/plain' in request.accept:
            request.override_renderer = 'string'

        self.setup_default_wrappers()

    def setup_default_wrappers(self):
        self._after_calls['index'] = [
            wrappers.wrap_in_dict(self.request),
            wrappers.add_meta(self.request),
        ]

        self._after_calls['show'] = [
            wrappers.wrap_in_dict(self.request),
        ]

        self._after_calls['delete'] = [
            wrappers.add_confirmation_url(self.request)
        ]

        self._after_calls['delete_many'] = [
            wrappers.add_confirmation_url(self.request)
        ]

        self._after_calls['update_many'] = [
            wrappers.add_confirmation_url(self.request)
        ]

    def __getattr__(self, attr):
        if attr in ACTIONS:
            return self.not_allowed_action

        raise AttributeError(attr)

    def not_allowed_action(self, *a, **k):
        raise JHTTPMethodNotAllowed()

    def add_before_or_after_call(self, action, _callable, pos=None, before=True):
        if not callable(_callable):
            raise ValueError('%s is not a callable' % _callable)

        if before:
            callkind = self._before_calls
        else:
            callkind = self._after_calls

        if pos is None:
            callkind[action].append(_callable)
        else:
            callkind[action].insert(pos, _callable)

    add_before_call = lambda self, *a, **k: self.add_before_or_after_call(*a, before=True, **k)
    add_after_call = lambda self, *a, **k: self.add_before_or_after_call(*a, before=False, **k)

    def subrequest(self, url, params={}, method='GET'):
        req =  Request.blank(url, cookies=self.request.cookies,
                            content_type='application/json',
                            method=method)

        if req.method == 'GET' and params:
            req.body = urllib.urlencode(params)

        if req.method == 'POST':
            req.body = json.dumps(params)

        return self.request.invoke_subrequest(req)

    def needs_confirmation(self):
        return '__confirmation' not in self._params

    def delete_many(self, **kw):
        if not self._model_class:
            log.error("%s _model_class in invalid: %s",
                    self.__class__.__name__, self._model_class)
            raise JHTTPBadRequest

        objs = self._model_class.get_collection(**self._params)

        if self.needs_confirmation():
            return objs

        count = len(objs)
        objs.delete()
        return JHTTPOk("Deleted %s %s objects" %
                            (count, self._model_class.__name__))

    def id2obj(self, name, model, id_field=None, setdefault=None):
        if name in self._params:
            if isinstance(self._params[name], model):
                return self._params[name]

            id_field = id_field or model._meta['id_field']
            obj = model.objects(**{id_field: self._params[name]}).first()
            if setdefault:
                self._params[name] = obj or setdefault
            else:
                if not obj:
                    raise JHTTPBadRequest('id2obj: Object %s not found' % self._params[name])
                self._params[name] = obj

