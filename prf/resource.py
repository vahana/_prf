import os
import logging
import types
from prf.utils import snake2camel, maybe_dotted, dictset

log = logging.getLogger(__name__)

DEFAULT_ID_NAME = 'id'

class Actions(object):
    index = '_index'
    show = '_show'
    create = '_create'
    update = '_update'
    patch = '_patch'
    delete = '_delete'
    delete_many = '_delete_many'
    update_many = '_update_many'

    @classmethod
    def all(cls):
        return cls.__dict__.keys()

def get_view_class(view, resource):
    '''Returns the dotted path to the default view class.'''

    view = maybe_dotted(view)
    if isinstance(view, types.TypeType):
        return view

    _, prefix_name = get_resource_elements(resource)
    parts = [a for a in prefix_name.split(':') if a]
    parts += [resource.collection_name or resource.member_name]

    view_file = '%s' % '_'.join(parts)
    view_class = '%sView' % snake2camel(view_file)

    if isinstance(view, types.ModuleType):
        return getattr(view, view_class)

    view = '%s:%s' % (view_file, view_class)
    return maybe_dotted('%s.views.%s' % (resource.config.package_name, view))


def get_resource_elements(resource):
    path_prefix = ''
    name_prefix = ''
    path_segs = []

    def get_path_pattern(res):
        if not res or (res and not res.path):
            return ''

        if res.id_name:
            id_full = res.id_name
        else:
            id_full = '%s_%s' % (res.member_name, DEFAULT_ID_NAME)

        return '%s/{%s}' % (res.path, id_full) if not res.is_singular else res.path

    path_prefix = '/'.join(filter(bool,
                            [resource.config.route_prefix,
                             get_path_pattern(resource.parent),
                             resource.prefix]))

    name_prefix = ':'.join(filter(bool,
                            [resource.config.route_prefix,
                             resource.parent.uid,
                             resource.prefix]))
    if name_prefix:
        name_prefix += ':'

    return path_prefix, name_prefix


def add_action_routes(config, view, member_name, collection_name, **kwargs):

    view = maybe_dotted(view)
    path_prefix = kwargs.pop('path_prefix', '')
    name_prefix = kwargs.pop('name_prefix', '')

    id_name = kwargs.pop('id_name', view._id_name) or DEFAULT_ID_NAME

    _acl = kwargs.pop('acl', view._acl)
    if _acl:
        _acl._id_name = id_name

    id_slug = ('/{%s}' % id_name if collection_name else '')
    path = os.path.join(path_prefix, (collection_name or member_name))

    _auth = config.registry.get('prf.auth', False)
    _traverse = kwargs.pop('traverse', None) or id_slug
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
        add_route_and_view(config, Actions.index, name_prefix + collection_name,
                           path, 'GET')

    add_route_and_view(config, Actions.show, name_prefix + member_name, path
                       + id_slug, 'GET', traverse=_traverse)

    add_route_and_view(config, Actions.update, name_prefix + member_name, path
                       + id_slug, 'PUT', traverse=_traverse)

    add_route_and_view(config, Actions.patch, name_prefix + member_name, path
                       + id_slug, 'PATCH', traverse=_traverse)

    add_route_and_view(config, Actions.create, name_prefix + (collection_name
                       or member_name), path, 'POST')

    add_route_and_view(config, Actions.delete, name_prefix + member_name, path
                       + id_slug, 'DELETE', traverse=_traverse)

    if collection_name:
        add_route_and_view(config, Actions.update_many, name_prefix
                           + (collection_name or member_name), path, 'PUT',
                           traverse=_traverse)

        add_route_and_view(config, Actions.delete_many, name_prefix
                           + (collection_name or member_name), path, 'DELETE',
                           traverse=_traverse)

    return path

class Resource(object):

    def __init__(self, config, member_name='', collection_name='',
                 parent=None, uid='', children=None, id_name='', prefix='',
                 http_cache=0, path=''):

        if parent and not member_name:
            raise ValueError('member_name can not be empty')

        self.config = config
        self.member_name = member_name
        self.collection_name = collection_name
        self.parent = parent
        self.id_name = id_name
        self.prefix = prefix
        self.http_cache = http_cache
        self.children = children or []
        self._ancestors = []
        self.uid = self.get_uid(collection_name or member_name)
        self.path = ''

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
    resource_map = property(lambda self: self.config.registry['prf.resources_map'])
    is_singular = property(lambda self: self.member_name \
                           and not self.collection_name)

    def get_uid(self, name=''):
        return ':'.join(filter(bool, [
                                self.parent.uid if self.parent else '',
                                self.prefix,
                                name or self.member_name]))

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

        prefix_from_member, _, member_name = member_name.rpartition('/')
        prefix = kwargs.pop('prefix', prefix_from_member)

        if collection_name == '':
            collection_name = member_name + 's'
        elif collection_name is None:
            collection_name = ''

        child_resource = Resource(self.config, member_name=member_name,
                                  collection_name=collection_name,
                                  parent=parent,
                                  prefix=prefix)

        child_view = get_view_class(kwargs.pop('view', None), child_resource)
        child_resource.id_name = kwargs.get('id_name', child_view._id_name)

        child_view._serializer = maybe_dotted(
                            kwargs.pop('serializer', child_view._serializer))

        root_resource = self.config.get_root_resource()

        kwargs['path_prefix'], kwargs['name_prefix'] = \
                            get_resource_elements(child_resource)

        # set some defaults
        kwargs.setdefault('renderer', child_view._default_renderer)
        kwargs.setdefault('http_cache', root_resource.http_cache)

        # add the routes for the resource
        path = add_action_routes(self.config, child_view, member_name,
                          collection_name, **kwargs)

        child_resource.add_to_resource_map(path)
        parent.children.append(child_resource)

        return child_resource

    def add_singular(self, *arg, **kw):
        kw['collection_name'] = None
        return self.add(*arg, **kw)

    def add_to_resource_map(self, path):
        def _add(key):
            if key in self.resource_map:
                r_ = self.resource_map[key]
                log.warning('Resource override: %s%s becomes %s%s' %\
                     (r_.config.package_name, path, self.config.package_name, path))

            self.resource_map[key] = self

        _add(self.get_uid())
        if self.collection_name:
            _add(self.get_uid(self.collection_name))

        self.path = path

