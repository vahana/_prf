import time
import json
import pytest
from prf.utils.dictset import dictset


BENCHMARK_OPTIONS = {
    'min_rounds': 10,
    'warmup': True,
    'disable_gc': True,
    'calibration_precision': 100,
    'timer': time.time,
}

def options(group):
    d = dict(BENCHMARK_OPTIONS)
    d.update({'group': group})
    return d


LOREM = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc ut dictum nibh, non congue dolor. Nulla sollicitudin nunc ac nisl vestibulum auctor. Sed molestie dignissim feugiat. Etiam placerat justo arcu, et euismod sapien varius eu. Maecenas pellentesque, sapien non sagittis dapibus, tellus odio porttitor est, sit amet congue lorem nisl in nulla. Nullam rhoncus nisl tellus, eu sodales ligula eleifend quis. Cras accumsan in purus sit amet efficitur. Mauris et felis varius, mollis massa vel, vestibulum turpis. Vivamus euismod libero a lacus ultricies, quis convallis mauris tempus. Vivamus commodo gravida hendrerit.'
# LOREM = 'L'


class TestDictSetBenchmark(object):
    sample_d = dictset(
        a=1,
        b=LOREM,
        c=[2] * 10,
        d={
            'i': [LOREM] * 10,
            'ii': {
                'aa': LOREM,
                'ab': [5, 6, 7],
            },
            'iii': [
                {'j': LOREM},
                {'jj': LOREM},
            ]
        },
        e={
            'i': LOREM,
            'ii': LOREM,
            'iii': LOREM,
            'iv': LOREM,
            'v': LOREM,
            'vi': LOREM,
            'vii': LOREM,
            'viii': LOREM,
            'ix': LOREM,
            'x': LOREM,
        }
    )

    @pytest.mark.benchmark(**options('flat'))
    def test_flat(self, benchmark):
        d = dictset(self.sample_d)
        benchmark(d.flat)

    @pytest.mark.benchmark(**options('flat'))
    def test_flat_lists(self, benchmark):
        d = dictset(self.sample_d)
        benchmark(d.flat, keep_lists=False)

    @pytest.mark.benchmark(**options('unflat'))
    def test_unflat(self, benchmark):
        d = dictset(self.sample_d).flat()
        benchmark(d.unflat)
        assert d.unflat() == self.sample_d

    @pytest.mark.benchmark(**options('unflat'))
    def test_unflat_lists(self, benchmark):
        d = dictset(self.sample_d).flat(keep_lists=False)
        benchmark(d.unflat)
        assert d.unflat() == self.sample_d

    @pytest.mark.benchmark(**options('extract'))
    def test_extract(self, benchmark):
        d = dictset(self.sample_d)
        args = ['a', 'b', 'c']
        benchmark(d.extract, args)
        assert d.extract(args) == {
            'a': self.sample_d['a'],
            'b': self.sample_d['b'],
            'c': self.sample_d['c'],
        }

    @pytest.mark.benchmark(**options('extract'))
    def test_extract_nested(self, benchmark):
        d = dictset(self.sample_d)
        args = ['a', 'b', 'c', 'd.ii.*']
        benchmark(d.extract, args)
        assert d.extract(['a', 'b', 'c', 'd.ii.*']) == {
            'a': self.sample_d['a'],
            'b': self.sample_d['b'],
            'c': self.sample_d['c'],
            'aa': self.sample_d['d']['ii']['aa'],
            'ab': self.sample_d['d']['ii']['ab'],
        }

    @pytest.mark.benchmark(**options('subset'))
    def test_subset(self, benchmark):
        d = dictset(self.sample_d)
        args = ['a', 'b', 'd']
        benchmark(d.subset, args)
        assert d.subset(args) == {
            'a': self.sample_d['a'],
            'b': self.sample_d['b'],
            'd': self.sample_d['d'],
        }

    @pytest.mark.benchmark(**options('update_with'))
    def test_update_with(self, benchmark):
        # Include d in both to have a collision
        d = dictset(self.sample_d).subset(['a', 'b', 'd'])
        e = dictset(self.sample_d).subset(['c', 'd', 'e'])
        benchmark(d.update_with, e)
        assert d.update_with(e) == self.sample_d

    @pytest.mark.benchmark(**options('update_with'))
    def test_update_with_append_to(self, benchmark):
        # Include d in both to have a collision
        d = dictset(self.sample_d).subset(['a', 'b', 'd'])
        e = dictset(self.sample_d).subset(['c', 'd', 'e'])
        a = []
        benchmark(d.update_with, e, append_to=a)
        assert d.update_with(e, append_to=a) == self.sample_d
