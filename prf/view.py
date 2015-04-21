import json
import logging
import urllib
from urlparse import urlparse
from pyramid.request import Request
from pyramid.response import Response

import prf.exc
from prf.utils import dictset, issequence, prep_params, process_fields,\
                      json_dumps

log = logging.getLogger(__name__)

ALLOWED_ACTIONS = ['index', 'show', 'create', 'update', 'delete',
                    'update_many', 'delete_many']

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
            return action(**matchdict)

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
    _serializer = None
    _acl = None
    _id_name = None

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self._model_class = None

        self.process_params()

        # no accept headers, use default
        if '' in request.accept:
            request.override_renderer = self._default_renderer
        elif 'application/json' in request.accept:

            request.override_renderer = 'prf_json'
        elif 'text/plain' in request.accept:

            request.override_renderer = 'string'

        self.init()

    def init(self):
        pass
        #to override in children

    def process_params(self):
        params = self.request.params.mixed()
        ctype = self.request.content_type
        if self.request.method in ['POST', 'PUT', 'PATCH']:
            if ctype == 'application/json':
                try:
                    params.update(self.request.json)
                except ValueError, e:
                    log.error("Excpeting JSON. Received: '%s'. Request: %s %s"
                              ,self.request.body, self.request.method, self.request.url)

        self._params = dictset()

        for key, val in params.items():
            try:
                self._params.extend(dictset.from_dotted(key, val))
            except:
                raise prf.exc.HTTPBadRequest('Can not mix dotted and regular param names')

        if self.request.method == 'GET':
            self._params.setdefault('_limit', 20)

    def __getattr__(self, attr):
        if attr in ALLOWED_ACTIONS:
            return self.not_allowed_action

        raise AttributeError(attr)

    def is_serializable(self, objs, many):
        if not self._serializer:
            return False

        if many:
            if not issequence(objs):
                return False
            elif len(objs) > 0:
                obj = objs[0]
            else:
                return True
        else:
            obj = objs

        return self._serializer._model and isinstance(obj, self._serializer._model)

    def serialize(self, objs, many=False):
        if not self.is_serializable(objs, many):
            return objs, len(objs)

        kw = {}
        fields = self._params.get('_fields')

        if fields is not None:
            kw['only'], kw['exclude'] = process_fields(fields)

        serializer = self._serializer(context={'request':self.request},
                                        many=many, strict=True, **kw)
        data = serializer.dump(objs).data

        if many:
            return data, len(data)
        else:
            return data

    def _index(self, **kw):
        objs = self.index(**kw)
        serielized, count = self.serialize(objs, many=True)

        total = getattr(objs, '_total', count)

        serielized = self.add_meta(serielized)

        return dict(
            total = total,
            count = count,
            data = serielized
        )

    def _show(self, **kw):
        obj = self.show(**kw)
        if isinstance(obj, dict):
            fields = self._params.get('_fields')
            return dictset(obj).subset(fields) if fields else obj

        return self.serialize(obj, many=False)

    def _create(self, **kw):
        obj = self.create(**kw)

        if not obj:
            return prf.exc.HTTPCreated()

        return prf.exc.HTTPCreated(
                        location=self.request.current_route_url(obj.id),
                        resource=self.serialize(obj, many=False))

    def _update(self, **kw):
        self.update(**kw)
        return prf.exc.HTTPOk()

    def _patch(self, **kw):
        self.patch(**kw)
        return prf.exc.HTTPOk()

    def _delete(self, **kw):
        self.delete(**kw)
        return prf.exc.HTTPOk()

    def _update_many(self, **kw):
        self.update_many(**kw)
        return prf.exc.HTTPOk()

    def _delete_many(self, **kw):
        self.delete_many(**kw)
        return prf.exc.HTTPOk()

    def not_allowed_action(self, *a, **k):
        raise prf.exc.HTTPMethodNotAllowed()

    def subrequest(self, url, params={}, method='GET'):
        req = Request.blank(url, cookies=self.request.cookies,
                            content_type='application/json', method=method)

        if req.method == 'GET' and params:
            req.body = urllib.urlencode(params)

        if req.method == 'POST':
            req.body = json_dumps(params)

        return self.request.invoke_subrequest(req)

    def needs_confirmation(self):
        return '__confirmation' not in self._params

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
                    each.setdefault('self', '%s/%s' % (url.geturl(),
                                    urllib.quote(str(each[self._id_name or 'id']))))
                except TypeError:
                    pass
        except (TypeError, KeyError):
            pass
        finally:
            return collection


class NoOp(BaseView):

    """Use this class as a stub if you want to layout all your resources before
    implementing actual views.
    """

    def index(self, **kw):
        return [dict(route=self.request.matched_route.name, kw=kw,
                params=self._params)]

    def show(self, **kw):
        return kw
