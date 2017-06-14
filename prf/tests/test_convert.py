import pytest
from prf.utils.convert import *
from prf.utils import dictset


class TestConvert():
    def test_parametrize(self):
        def func(dset, value):
            return value

        wrapped = parametrize(func)

        with pytest.raises(DKeyError):
            wrapped(dictset(), None)

        assert 10 == wrapped(dictset(), None, default=10)

    def test_bool(self):
        d_ = dict(a=1, b=True, c=False, d='true', e='false', f='BLABLA', g=None)

        with pytest.raises(DKeyError):
            asbool(dict(), 'a')

        assert asbool(d_, 'a') is True
        assert asbool(d_, 'a') is True
        assert asbool(d_, 'b') is True
        assert asbool(d_, 'c') is False
        assert asbool(d_, 'd') is True
        assert asbool(d_, 'e') is False
        assert asbool(d_, 'g') is False

        with pytest.raises(DValueError):
            asbool(d_, 'f')

        with pytest.raises(DKeyError):
            asbool(d_, 'NOTHERE')

        assert asbool(d_, 'NOTHERE', default=False) is False

    def test_list(self):
        with pytest.raises(DKeyError):
            aslist(dict(), 'a')

        assert aslist(dict(a=''), 'a') == []
        assert aslist(dict(a='a'), 'a') == ['a']
        assert aslist(dict(a='a1,a2'), 'a') == ['a1', 'a2']
        assert aslist(dict(a='a,'), 'a') == ['a']

        with pytest.raises(DValueError):
            aslist(dict(a=''), 'a', raise_on_empty=True)

        d_ = dict(a='a', b='')
        aslist(d_, 'a', pop=True)
        assert d_ == dict(b='')

        assert aslist(dict(a='a,b,a'), 'a', unique=True) == ['a','b']

    @pytest.mark.skip('Fix ME')
    def test_list1(self):
        assert aslist(dict(a='a,'), 'a', remove_empty=False) == ['a', '']

    def test_int(self):
        with pytest.raises(DKeyError):
            asint(dict(), 'a')

        assert asint(dict(a='1'), 'a') == 1

    def test_float(self):
        with pytest.raises(DKeyError):
            asfloat(dict(), 'a')

        assert asfloat(dict(a='1.1'), 'a') == 1.1

    def test_dict(self):
        with pytest.raises(DKeyError):
            asdict(dict(), 'a')

        assert asdict(dict(a='a'), 'a') == {'a':''}
        assert asdict(dict(a=''), 'a') == {}

        assert asdict(dict(a='a:1'), 'a') == dict(a='1')
        assert asdict(dict(a='a:1'), 'a', _type=int) == dict(a=1)

        assert asdict(dict(a='a:1, b:2'), 'a') == dict(a='1', b='2')
        assert asdict(dict(a='a:1,b:2,a:2,a:3'), 'a') == dict(a=['1', '2', '3'], b='2')

        _d = dict(a='b:1')
        asdict(_d, 'a', _type=int, _set=True)
        assert _d == dict(a=dict(b=1))

        _d = dict(a='b:1')
        asdict(_d, 'a', _type=int, pop=True)
        assert _d == dict()

    def test_datetime(self):
        from datetime import datetime

        with pytest.raises(DKeyError):
            asdt(dict(), 'a')

        with pytest.raises(DValueError):
            asdt(dict(a='asdfasdf'), 'a')

        assert asdt(dict(a='2000-01-01T01:01:01'), 'a') == datetime(2000,01,01,01,01,01)

