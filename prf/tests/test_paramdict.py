from prf.utils.params import paramdict
from slovar.errors import DKeyError


class TestParamDict(object):
    def test_exc(self):
        params = paramdict(a=1)
        try:
            params.b
        except DKeyError:
            pass
        else:
            raise Exception('Expected and exception')
