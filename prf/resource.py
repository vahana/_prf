import os
import logging
import types
from prf.utils import snake2camel, maybe_dotted

log = logging.getLogger(__name__)

DEFAULT_ID_NAME = 'id'

class Action(object):
    INDEX = '_index'
    SHOW = '_show'
    CREATE = '_create'
    UPDATE = '_update'
    DELETE = '_delete'
    DELETE_MANY = '_delete_many'
    UPDATE_MANY = '_update_many'


def get_view_class(view_param, resource):
    '''Returns the dotted path to the default view class.'''

    parts = [a.member_name for a in resource.ancestors] \
        + [resource.collection_name or resource.member_name]
    if resource.prefix:
        parts.insert(-1, resource.prefix)

    view_file = '%s' % '_'.join(parts)
    view_class = '%sView' % snake2camel(view_file)

    view = maybe_dotted(view_param)
    if isinstance(view, types.TypeType):
        return view

    elif isinstance(view, types.ModuleType):
        return getattr(view, view_class)

    view = '%s:%s' % (view_file, view_class)
    return maybe_dotted('%s.views.%s' % (resource.config.package_name, view))


def get_uri_elements(resource):
    # figure out the url name and paths prefixes
    path_prefix = ''
    name_prefix = ''
    path_segs = []

    for res in resource.ancestors:
        if not res.is_singular:
            if res.id_name:
                id_full = res.id_name
            else:
                id_full = '%s_%s' % (res.member_name, DEFAULT_ID_NAME)

            path_segs.append('%s/{%s}' % (res.collection_name, id_full))
        else:
            path_segs.append(res.member_name)

    if path_segs:
        path_prefix = '/'.join(path_segs)

    if resource.prefix:
        path_prefix += '/' + resource.prefix

    name_segs = [a.member_name for a in resource.ancestors]
    name_segs.insert(1, resource.prefix)
    name_segs = filter(bool, name_segs)
    if name_segs:
        name_prefix = '_'.join(name_segs) + ':'

    if resource.config.route_prefix:
        name_prefix = '%s_%s' % (resource.config.route_prefix.replace('/', '_'),
                                 name_prefix)

    return path_prefix, name_prefix


def add_action_routes(config, view, member_name, collection_name, **kwargs):

    view = maybe_dotted(view)
    path_prefix = kwargs.pop('path_prefix', '')
    name_prefix = kwargs.pop('name_prefix', '')

    id_name = ('/{%s}' % (kwargs.pop('id_name', None)
               or DEFAULT_ID_NAME) if collection_name else '')

    path = os.path.join(path_prefix, (collection_name or member_name))

    _acl = kwargs.pop('acl', view._acl)
    _auth = config.registry._auth
    _traverse = kwargs.pop('traverse', None) or id_name

    added_routes = {}

    def add_route_and_view(config, action, route_name, path, request_method,
                           **route_kwargs):
        if _acl:
            route_kwargs['factory'] = _acl

        if route_name not in added_routes:
            config.add_route(route_name, path, **route_kwargs)
            added_routes[route_name] = path

        config.add_view(view=view, attr=action, route_name=route_name,
                        request_method=request_method,
                        permission=(action if _auth else None),
                        **kwargs)
        config.commit()

    if collection_name:
        add_route_and_view(config, Action.INDEX, name_prefix + collection_name,
                           path, 'GET')

    add_route_and_view(config, Action.SHOW, name_prefix + member_name, path
                       + id_name, 'GET', traverse=_traverse)

    add_route_and_view(config, Action.UPDATE, name_prefix + member_name, path
                       + id_name, 'PUT', traverse=_traverse)

    add_route_and_view(config, Action.UPDATE, name_prefix + member_name, path
                       + id_name, 'PATCH', traverse=_traverse)

    add_route_and_view(config, Action.CREATE, name_prefix + (collection_name
                       or member_name), path, 'POST')

    add_route_and_view(config, Action.DELETE, name_prefix + member_name, path
                       + id_name, 'DELETE', traverse=_traverse)

    if collection_name:
        add_route_and_view(config, Action.UPDATE_MANY, name_prefix
                           + (collection_name or member_name), path, 'PUT',
                           traverse=_traverse)

        add_route_and_view(config, Action.DELETE_MANY, name_prefix
                           + (collection_name or member_name), path, 'DELETE',
                           traverse=_traverse)

    return path

class Resource(object):

    def __init__(self, config, member_name='', collection_name='',
                 parent=None, uid='', children=None, id_name='', prefix='',
                 http_cache=0):

        self.config = config
        self.member_name = member_name
        self.collection_name = collection_name
        self.parent = parent
        self.uid = uid
        self.id_name = id_name
        self.prefix = prefix
        self.http_cache = http_cache
        self.children = children or []
        self._ancestors = []

    def __repr__(self):
        return "%s(uid='%s')" % (self.__class__.__name__, self.uid)

    def get_ancestors(self):
        '''Returns the list of ancestor resources.'''

        if self._ancestors:
            return self._ancestors

        if not self.parent:
            return []

        obj = self.resource_map.get(self.parent.uid)

        while obj and obj.member_name:
            self._ancestors.append(obj)
            obj = obj.parent

        self._ancestors.reverse()
        return self._ancestors

    ancestors = property(get_ancestors)
    resource_map = property(lambda self: self.config.registry._resources_map)
    is_singular = property(lambda self: self.member_name \
                           and self.collection_name is None)

    def add(self, member_name, collection_name='', **kwargs):
        """
        :param member_name: singular name of the resource.

        :param collection_name: plural name of the resource.
            Note: if collection_name is empty, it means resource is singular

        :param kwargs:
            view: custom view to overwrite the default one.
            the rest of the keyward arguments are passed to add_action_routes call.

        :return: ResourceMap object
        """

        parent = self
        prefix = kwargs.pop('prefix', '')

        if collection_name == '':
            collection_name = member_name + 's'
        elif collection_name is None:
            collection_name = ''

        uid = ':'.join(filter(bool, [parent.uid, prefix, member_name]))

        child_resource = Resource(self.config, member_name=member_name,
                                  collection_name=collection_name,
                                  parent=parent, uid=uid,
                                  id_name=kwargs.get('id_name', ''),
                                  prefix=prefix)

        child_view = get_view_class(kwargs.pop('view', None), child_resource)
        child_view._serializer = maybe_dotted(
                            kwargs.pop('serializer', child_view._serializer))

        root_resource = self.config.get_root_resource()

        kwargs['path_prefix'], kwargs['name_prefix'] = \
                            get_uri_elements(child_resource)

        # set some defaults
        kwargs.setdefault('renderer', child_view._default_renderer)
        kwargs.setdefault('http_cache', root_resource.http_cache)

        # add the routes for the resource
        path = add_action_routes(self.config, child_view, member_name,
                          collection_name, **kwargs)

        # self.add_to_resource_map('%s:%s' % (kwargs['name_prefix'], uid),
        #                             path, child_resource)
        self.add_to_resource_map(uid, path, child_resource)
        parent.children.append(child_resource)

        return child_resource

    def add_to_resource_map(self, key, path, child_resource):
        if key in self.resource_map:
            r_ = self.resource_map[key]
            log.warning('Resource override: %s%s becomes %s%s' %\
                 (r_.config.package_name, path, self.config.package_name, path))

        self.resource_map[key] = child_resource

    def add_from(self, resource, **kwargs):
        '''add a resource with its all children resources to the current resource'''

        new_resource = self.add(resource.member_name,
                                resource.collection_name, **kwargs)
        for child in resource.children:
            new_resource.add_from(child, **kwargs)
