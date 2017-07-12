import pytest
from prf.utils.params import paramdict
from slovar.errors import DKeyError


class TestParamDict(object):
    def test_exc(self):
        params = paramdict(a=1)
        with pytest.raises(DKeyError):
            params.b
