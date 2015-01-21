import unittest
from prf.utils.convert import *


class TestConvert(unittest.TestCase):

    def test_bool(self):
        d_ = dict(a=1, b=True, c=False, d='true', e='false', f='BLABLA')
        self.assertTrue(asbool(d_, 'a'))
        self.assertTrue(asbool(d_, 'b'))
        self.assertFalse(asbool(d_, 'c'))
        self.assertTrue(asbool(d_, 'd'))
        self.assertFalse(asbool(d_, 'e'))
        self.assertFalse(asbool(d_, 'f'))

        self.assertFalse(asbool(d_, 'NOTHERE', default=False))

        asbool(d_, 'NOTHERE', default=False) == False


