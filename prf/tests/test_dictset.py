import unittest
from prf.utils import dictset


class TestDictSet(unittest.TestCase):

    def test(self):
        dset = dictset(a=1)
        self.assertTrue(isinstance(dset, dict))

        self.assertEqual(dset.a, dset['a'])

        dset.a = 10
        self.assertEqual(dset['a'], 10)

        dset.b = 2
        self.assertEqual(dset['b'], dset.b)

        dset.d = dict(a=1)
        self.assertEqual(dset.d.a, dset['d']['a'])

        del dset.b
        self.assertTrue('b' not in dset)

    def test_subset(self):
        dset = dictset(a=1, b=2, c=3)
        self.assertEqual(set(dset.subset(['a', 'c']).keys()), set(['a', 'c']))

        self.assertEqual(set(dset.subset(['-a']).keys()), set(['b', 'c']))

        # can not have both negative and positive.
        self.assertRaises(Exception, dset.subset, ['-a', 'b'])

        self.assertEqual(dset.subset(['NOTTHERE']), {})
        self.assertEqual(dset.subset(['-NOTTHERE']), dset)
        self.assertEqual(dset.subset([]), {})

        self.assertEqual(set(dset.subset(['a', 'NOTTHERE']).keys()), set(['a'
                         ]))
        self.assertEqual(set(dset.subset(['-a', '-NOTTHERE']).keys()), set(['b'
                         , 'c']))

    def test_remove(self):
        dset = dictset(a=1, b=2, c=3)

        self.assertEqual(dset.remove([]), dset)
        self.assertEqual(dset.remove(['NOTTHERE']), dset)
        self.assertEqual(dset.remove(['b', 'c']), dict(a=1))

    def test_update(self):
        dset = dictset(a=1, b=2, c=3)
        self.assertEqual(dset.update(dict(d=4)).d, 4)
        self.assertEqual(dset.d, 4)

    def test_copy(self):
        dset = dictset(a=1, b=2, c=3)
        dset_copy = dset.copy()
        dset_alias = dset

        self.assertEqual(dset, dset_copy)
        self.assertEqual(id(dset), id(dset_alias))
        self.assertNotEqual(id(dset), id(dset_copy))

    def test_pop_by_values(self):
        dset = dictset(a=1, b=2, c=2)
        dset_copy = dset.copy()
        dset.pop_by_values(666)
        self.assertEqual(dset, dset_copy)

        dset.pop_by_values(2)
        self.assertEqual(dset.keys(), ['a'])
        self.assertNotEqual(dset, dset_copy)
