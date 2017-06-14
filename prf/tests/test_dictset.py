import pytest
from datetime import datetime
from prf.utils.dictset import dictset, merge
from prf.utils.utils import DKeyError, DValueError


class TestDictSet():

    def test(self):
        dset = dictset(a=1)

        assert isinstance(dset, dict) is True
        assert dset.a == dset['a']

        dset.a = 10
        assert dset['a'] == 10

        dset.b = 2
        assert dset['b'] == dset.b

        dset.d = dict(a=1)
        assert dset.d.a == dset['d']['a']

        del dset.b
        assert 'b' not in dset

    def test_subset(self):
        dset = dictset(a=1, b=2, c=3)

        assert set(dset.subset(['a', 'c']).keys()) == set(['a', 'c'])
        assert set(dset.subset(['-a']).keys()) == set(['b', 'c'])

        # can not have both negative and positive.
        with pytest.raises(Exception):
            dset.subset(['-a', 'b'])

        assert dset.subset(['NOTTHERE']) == {}
        assert dset.subset(['-NOTTHERE']) == dset
        assert dset.subset([]) == {}

        assert set(dset.subset(['a', 'NOTTHERE']).keys()) == set(['a'])
        assert set(dset.subset(['-a', '-NOTTHERE']).keys()) == set(['b', 'c'])

    def test_remove(self):
        dset = dictset(a=1, b=2, c=3)

        assert dset.remove([]) == dset
        assert dset.remove(['NOTTHERE']) == dset
        assert dset.remove(['b', 'c']) == dict(a=1)

    def test_update(self):
        dset = dictset(a=1, b=2, c=3)
        assert dset.update(dict(d=4)).d == 4
        assert dset.d == 4

    def test_copy(self):
        dset = dictset(a=1, b=2, c=3)
        dset_copy = dset.copy()
        dset_alias = dset

        assert dset == dset_copy
        assert id(dset) == id(dset_alias)
        assert id(dset) != id(dset_copy)

    def test_pop_by_values(self):
        dset = dictset(a=1, b=2, c=2)
        dset_copy = dset.copy()
        dset.pop_by_values(666)
        assert dset == dset_copy

        dset.pop_by_values(2)
        assert dset.keys() == ['a']
        assert dset != dset_copy

    def test_merge(self):
        d1 = {}
        merge(d1, {})
        assert d1 == {}

        merge(d1, dict(a=1))
        assert d1 == dict(a=1)

        # XXX This doesn't raise anymore. It should.
        d1 = dict(a={})
        # with pytest.raises(ValueError):
        #     merge(d1, dict(a=1))

        merge(d1, dict(a={}))
        assert d1 == dict(a={})

        merge(d1, dict(a=dict(b=1)))
        assert d1 == dict(a=dict(b=1))

        d1 = dict(a=dict(c=1))
        merge(d1, dict(a=dict(b=1)))
        assert d1 == {'a': {'c': 1, 'b': 1}}

        d1 = dictset(a={})
        d1.merge({})

    def test__getattr__(self):
        d1 = dictset()
        with pytest.raises(AttributeError):
            d1.NOTTHERE
        d1['a'] = 1

    def test__contains__(self):
        d1 = dictset(a=dict(b=1))
        assert ['a', 'b'] in d1

    def test_to_dictset(self):
        d1 = dictset(a=[dict(c=1), 1])
        assert isinstance(d1.a[0], dictset)

    def test_get_tree(self):
        d1 = dictset({'a.b':1, 'a.c':2})
        assert d1.get_tree('a') == {'c': 2, 'b': 1}

    def test_from_dotted(self):
        assert dictset.from_dotted('a.b.c', 1) == {'a': {'b': {'c': 1}}}

    def test_has(self):
        d1 = dictset(a=1)
        with pytest.raises(DValueError):
            d1.has('a', check_type=basestring)

        assert d1.has('a', check_type=int) == True

        with pytest.raises(DValueError):
            d1.has('b', check_type=int)

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
