import pytest
from datetime import datetime
from slovar import slovar

from prf.utils.utils import *
from prf.utils import process_fields

class TestUtils(object):

    def test_JSONEncoder(self):
        assert "1685-03-31T01:01:01" in json_dumps(
            dict(a=datetime(1685,0o3,31,0o1,0o1,0o1,0o1)))

    def test_split_strip(self):
        assert split_strip('') == []
        assert split_strip('a,  ') == ['a']
        assert split_strip('a,  b,') == ['a','b']

    def test_process_limit(self):
        with pytest.raises(ValueError):
            process_limit(None, None, None)

        with pytest.raises(ValueError):
            process_limit(0, 0, 0)

        with pytest.raises(ValueError):
            process_limit(-1, None, 1)

        with pytest.raises(ValueError):
            process_limit(None, -1, 1)

        with pytest.raises(ValueError):
            process_limit(None, 'aaa', 'dsfsadf')

        assert (0, 0) == process_limit(None, None, 0)

        assert (0, 0) == process_limit(0, None, 0)
        assert (0, 10) == process_limit(0, None, 10)
        assert (1, 10) == process_limit(1, None, 10)

        assert (0, 10) == process_limit(None, 0, 10)
        assert (10, 10) == process_limit(None, 1, 10)
        assert (20, 10) == process_limit(None, 2, 10)

    def test_process_fields(self):
        assert process_fields('') == process_fields(None)
        _d = process_fields('a')
        assert _d['only'] == ['a']

        _d = process_fields('a, -b')
        assert _d['only'] == ['a']
        assert _d['exclude'] == ['b']

        _d = process_fields('a__as__b')
        assert _d['only'] == ['a']
        assert _d['show_as'] == {'a': 'b'}

        _d = process_fields('a.x')
        assert _d['only'] == ['a']
        assert _d['nested'] == {'a.x': 'a'}

        _d = process_fields('*')
        assert _d['star'] == True

    def test_snake2camel(self):
        assert snake2camel('a_b') == 'AB'
        assert snake2camel('aa_bb') == 'AaBb'

    def test_parse_specials(self):
        _specials = [
            '_asdict',
            '_count',
            '_distinct',
            '_end',
            '_explain',
            '_fields',
            '_flat',
            '_frequencies',
            '_group',
            '_ix',
            '_join',
            '_limit',
            '_page',
            '_scalar',
            '_sort',
            '_start',
            '_unwind',
            '_where'
        ]

        pp, sp = parse_specials(slovar(_limit=1))
        pp, sp = parse_specials(
            slovar({
                'a':'1',
                'b':2,
                'c.c':3,
                'd__d':5,
                'e__asint': '6',

                '_count':1,
                '_fields':'a,b',
                '_limit':10,
                '_sort':'-a,b',
                '_distinct':'abc',
                '_scalar':'a,b',
                '_group':'x,y'
            })
        )

        assert sp['_fields'] == ['a', 'b']
        assert sp['_sort'] == ['-a', 'b']

        assert pp['a'] == '1'
        assert pp['b'] == 2
        assert pp['c__c'] == 3
        assert 'c.c' not in pp
        assert pp['d__d'] == 5
        assert pp['e'] == 6


    def test_cleanup_url(self):
        from prf.utils import cleanup_url

        with pytest.raises(ValueError):
            cleanup_url('')

        with pytest.raises(ValueError):
            cleanup_url('/xyz/')

        assert 'abc' == cleanup_url('abc')
        assert 'abc.com/xyz' == cleanup_url('abc.com/xyz/')
        assert 'abc.com/xyz' == cleanup_url('abc.com/xyz?a=2&b=3')

        assert '' == cleanup_url('', _raise=False)
        assert '' == cleanup_url('/xyz/', _raise=False)

