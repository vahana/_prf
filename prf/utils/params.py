from prf.utils.dictset import dictset


class paramdict(dictset):
    def raise_getattr_exc(self, error):
        raise self.DKeyError(error)
