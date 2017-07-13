import pytest
from datetime import datetime
from prf.utils import dictset, dkdict
from slovar.errors import DKeyError, DValueError


class TestDictSet():
    def test_asbool(self):
        assert dictset(a=True).asbool('a') == True

    def test_aslist(self):
        assert dictset(a=[]).aslist('a') == []

    def test_asint(self):
        assert dictset(a=1).asint('a') == 1

    def test_asfloat(self):
        assert dictset(a='1.1').asfloat('a') == 1.1

    def test_asdict(self):
        assert dictset(a='a:1').asdict('a') == {'a':'1'}

    def test_asdt(self):
        assert dictset(a='2000-01-01T01:01:01').asdt('a') == datetime(2000,01,01,01,01,01)

    def test_dkdict(self):
        params = dkdict(a=1)
        with pytest.raises(DKeyError):
            params.b
