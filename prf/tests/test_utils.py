import pytest
from datetime import datetime
from prf.utils.utils import *

class TestUtils(object):

    def test_JSONEncoder(self):
        assert "2000-01-01T01:01:01Z" in json_dumps(
            dict(a=datetime(2000,01,01,01,01,01,01)))

    def test_split_strip(self):
        assert split_strip('') == []
        assert split_strip('a,  ') == ['a']
        assert split_strip('a,  b,') == ['a','b']

    def test_process_limit(self):
        with pytest.raises(DValueError):
            process_limit(None,None,None)

        with pytest.raises(DValueError):
            process_limit(0,0,0)

        with pytest.raises(DValueError):
            process_limit(0,None,-1)

        with pytest.raises(DValueError):
            process_limit(-1,None,1)

        with pytest.raises(DValueError):
            process_limit(None,-1,1)

        with pytest.raises(DValueError):
            process_limit(None,'aaa','dsfsadf')

        assert (0,0) == process_limit(None,None,0)

        assert (0,0) == process_limit(0,None,0)
        assert (0,10) == process_limit(0,None,10)
        assert (1,10) == process_limit(1,None,10)

        assert (0,10) == process_limit(None,0,10)
        assert (10,10) == process_limit(None,1,10)
        assert (20,10) == process_limit(None,2,10)

    def test_expand_list(self):
        assert expand_list(None) == []

        assert expand_list('1,2,3') == ['1', '2', '3']
        assert expand_list([1,2,3]) == [1,2,3]
        assert expand_list([1,2,[3,4]]) == [1,2,3,4]

        assert expand_list([1,2,'3,4']) == [1,2,'3','4']

    def test_process_fields(self):
        assert ([], []) == process_fields(None)
        assert ([], []) == process_fields('')
        assert (['a'], []) == process_fields('a')
        assert (['a'], ['b']) == process_fields('a, -b')
        assert ([], ['b']) == process_fields('-b')

    def test_snake2camel(self):
        assert snake2camel('a_b') == 'AB'
        assert snake2camel('aa_bb') == 'AaBb'

    def test_maybe_dotted(self):
        with pytest.raises(ImportError):
            maybe_dotted('aa')

        with pytest.raises(ValueError):
            maybe_dotted('')

        with pytest.raises(ValueError):
            maybe_dotted('.view')

        import prf
        assert maybe_dotted('prf.view') == prf.view

        assert maybe_dotted('prf.view:BaseView') == prf.view.BaseView

        maybe_dotted('XYZ', throw=False)

    def test_issequence(self):
        assert issequence('') is False
        assert issequence(1) is False
        assert issequence([]) is True
        assert issequence(dict) is True
        assert issequence(set) is True
        assert issequence(tuple) is True

    def test_prep_params(self):
        assert ({}, {'_count': False, 
                     '_fields': [], 
                     '_limit': 1, 
                     '_offset': 0, 
                     '_sort': []}) == prep_params({})

        assert prep_params(
            dict(
                a=1,b=3,
                _count=1,
                _fields='a,b',
                _limit=10,
                _sort = '-a,b'
            )) == (
                dict(a = 1, b = 3),
                dict(_count = True, 
                     _fields =  ['a', 'b'], 
                    _limit = 10, 
                    _offset = 0, 
                    _sort = ['-a', 'b']))
