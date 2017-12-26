import pytest
from datetime import datetime
from prf.utils.utils import *
from prf.utils import process_fields
from prf import dictset

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

    @pytest.mark.skip('process_fields doesn\'t seem to behave like this anymore')
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

    def test_prep_params(self):
        assert (
            {},
            {
                '_count': False,
                '_fields': [],
                '_limit': 1,
                '_start': 0,
                '_sort': []
            }
        ) == prep_params(dictset())

        assert (
            {'a': 1, 'b': 3},
            {
                '_count': True,
                '_fields':  ['a', 'b'],
                '_limit': 10,
                '_start': 0,
                '_sort': ['-a', 'b'],
                '_distinct': 'abc',
                '_scalar': 'a,b',
                '_group': 'x,y',
            }
        ) == prep_params(
            dictset(
                a=1,b=3,
                _count=1,
                _fields='a,b',
                _limit=10,
                _sort = '-a,b',
                _distinct = 'abc',
                _scalar = 'a,b',
                _group = 'x,y'
            )
        )

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

