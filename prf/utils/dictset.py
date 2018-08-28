from slovar import slovar as dictset
from prf.utils.errors import DKeyError, DValueError

class dkdict(dictset):
    def raise_getattr_exc(self, error):
        raise DKeyError(error)

    def raise_value_exc(self, error):
        raise DValueError(error)

