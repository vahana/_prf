from slovar import slovar
import prf.exc


class DynamicSchema(object):
    def __init__(self, **kw):
        self.only = []
        self.exclude = []
        self.__dict__.update(kw)

    def dump(self, objs):
        flat = self.context.get('flat')
        pop_empty = self.context.get('pop_empty')

        def to_dict(obj):
            if isinstance(obj, dict):
                d_ = slovar(obj).extract(self.context.get('fields'))
            if hasattr(obj, 'to_dict'):
                d_ = obj.to_dict(self.context.get('fields'))
            else:
                return obj

            if pop_empty:
                d_ = d_.flat(keep_lists=True).pop_by_values([[], {}, '']).unflat()
            if flat:
                d_ = d_.flat(keep_lists=not flat=='all')
            return d_

        try:
            if self.many:
                objs = [to_dict(each) for each in objs]
            else:
                objs = to_dict(objs)

            return slovar(data=objs)
        except AttributeError as e:
            raise prf.exc.HTTPBadRequest('%s can not be serialized: %s.' %\
                         ('Collection' if self.many else 'Resource', e))

