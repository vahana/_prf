import pytest
from datetime import datetime
from prf.utils.csv import dict2tab
from slovar import slovar


class TestCSV():
    def test_dict2tab(self):
        dict2tab(None)
        dict2tab([])

        #no fields passed
        assert '\r\n' == dict2tab([{'a':1}])

        #passing field `a`
        assert 'a\r\n1\r\n' == dict2tab([{'a':1}], 'a')

        #passing fields `a__as__A` but data is still with `a`
        assert 'A\r\n1\r\n' != dict2tab([{'a':1}], 'a__as__A')
        # assert 'A\r\n""\r\n' == dict2tab([{'a':1}], 'a__as__A')

        #passing fields `a__as__A`
        assert 'A\r\n1\r\n' == dict2tab([{'A':1}], 'a__as__A')


