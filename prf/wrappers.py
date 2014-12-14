import urllib
from datetime import datetime, date
from urlparse import urlparse
import logging

log = logging.getLogger(__name__)


class ValidationError(Exception):

    pass


def issequence(arg):
    """Return True if `arg` acts as a list and does not look like a string."""
    return not hasattr(arg, 'strip') and hasattr(arg, '__getitem__') \
        or hasattr(arg, '__iter__')


# Decorators

class wrap_me(object):

    """Base class for decorators used to add before and after calls.
    The callables are appended to the ``before`` or ``after`` lists,
    which are in turn injected into the method object being decorated.
    Method is returned without any side effects.
    """

    def __init__(self, before=None, after=None):

        self.before = (before if type(before)
                       is list else ([before] if before else []))
        self.after = (after if type(after)
                      is list else ([after] if after else []))

    def __call__(self, meth):
        if not hasattr(meth, '_before_calls'):
            meth._before_calls = []
        if not hasattr(meth, '_after_calls'):
            meth._after_calls = []

        meth._before_calls += self.before
        meth._after_calls += self.after

        return meth


class validator(wrap_me):

    """Decorator that validates the type and required fields in request params against the supplied kwargs

    ::

        class MyView():
            @validator(first_name={'type':int, 'required':True})
            def index(self):
                return response
    """

    def __init__(self, **kwargs):
        wrap_me.__init__(self, before=[validate_types(**kwargs),
                         validate_required(**kwargs)])


class callable_base(object):

    """Base class for all before and after calls.
    ``__eq__`` method is overloaded in order to prevent duplicate callables of the same type.
    For example, you could have a before call ``pager`` which is called in the base class and
    also decorate the action with ``paginate``. ``__eq__`` declares all same type callables to be the same.
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __eq__(self, other):
        '''we only allow one instance of the same type of callable.'''
        return type(self) == type(other)


# Before calls

class validate_base(callable_base):

    """Base class for validation callables.
    """

    def __call__(self, **kwargs):
        self.request = kwargs['request']
        self.params = self.request.params.copy()
        # tunneling internal param, no need to check.
        self.params.pop('_method', None)


class validate_types(validate_base):

    """

    Validates the field types in ``request.params`` match the types declared in ``kwargs``.
    Raises ValidationError if there is mismatch.
    """

    def __call__(self, **kwargs):
        validate_base.__call__(self, **kwargs)
        # checking the types
        for name, value in self.params.items():
            if value == 'None':  # fix this properly.
                continue
            _type = self.kwargs.get(name, {}).get('type')
            try:
                if _type == datetime:
                    # must be in iso format
                    value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
                elif _type == date:
                    # must be in iso format
                    value = datetime.strptime(value, '%Y-%m-%d')
                elif _type == None:
                    log.debug('Incorrect or unsupported type for %s(%s)',
                              name, value)
                    continue
                elif type(_type) is type:
                    _type(value)
                else:
                    raise ValueError
            except ValueError, e:
                raise ValidationError('Bad type %s for %s=%s. Suppose to be %s'
                                       % (type(value), name, value, _type))


class validate_required(validate_base):

    """Validates that fields in ``request.params`` are present
    according to ``kwargs`` argument passed to ``__call__.
    Raises ValidationError in case of the mismatch
    """

    def __call__(self, **kwargs):
        validate_base.__call__(self, **kwargs)
        # get parent resources' ids from matchdict, so there is no need to pass
        # in the request.params
        self.params.update(self.request.matchdict)

        self.kwargs.pop('id', None)

        required_fields = set([n for n in self.kwargs.keys()
                              if self.kwargs[n].get('required', False)])

        if not required_fields.issubset(set(self.params.keys())):
            raise ValidationError('Required fields: %s. Received: %s'
                                  % (list(required_fields),
                                  self.params.keys()))


# After calls.

class obj2dict(object):

    def __init__(self, request):
        self.request = request

    def __call__(self, **kwargs):
        '''converts objects in `result` into dicts'''

        result = kwargs['result']
        if isinstance(result, dict):
            return result

        _fields = kwargs.get('_fields', [])
        if hasattr(result, '_prf_meta'):
            _fields = result._prf_meta.get('fields', [])

        if hasattr(result, 'to_dict'):
            return result.to_dict(keys=_fields, request=self.request)
        elif issequence(result):

            # make sure its mutable, i.e list
            result = list(result)
            for ix, each in enumerate(result):
                result[ix] = obj2dict(self.request)(_fields=_fields,
                        result=each)

        return result


class wrap_in_dict(object):

    def __init__(self, request):
        self.request = request

    def __call__(self, **kwargs):
        '''if result is a list then wrap it in the dict'''
        result = kwargs['result']

        if hasattr(result, '_prf_meta'):
            _meta = result._prf_meta
        else:
            _meta = {}

        result = obj2dict(self.request)(**kwargs)

        if isinstance(result, dict):
            return result
        else:
            result = {'data': result}
            result.update(_meta)

        return result


class add_meta(object):

    def __init__(self, request):
        self.request = request

    def __call__(self, **kwargs):
        result = kwargs['result']

        try:
            result['count'] = len(result['data'])
            for each in result['data']:
                try:
                    url = \
                        urlparse(self.request.current_route_url())._replace(query=''
                            )
                    each.setdefault('self', '%s/%s' % (url.geturl(),
                                    urllib.quote(str(each['id']))))
                except TypeError:
                    pass
        except (TypeError, KeyError):
            pass
        finally:
            return result


class add_confirmation_url(object):

    def __init__(self, request):
        self.request = request

    def __call__(self, **kwargs):
        result = kwargs['result']
        q_or_a = ('&' if self.request.params else '?')

        return dict(method=self.request.method, count=len(result),
                    confirmation_url=self.request.url
                    + '%s__confirmation&_m=%s' % (q_or_a, self.request.method))
