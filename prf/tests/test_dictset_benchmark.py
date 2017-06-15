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


class TestDictSetBenchmark(object):
    sample_d = dictset(
        a=1,
        b=LOREM,
        c=[2] * 10,
        d={
            'i': [LOREM] * 10,
            'ii': {
                '1.': LOREM,
                '2.': [5, 6, 7],
            }
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

    def json_print(self, v):
        print json.dumps(v, indent=2)

    @pytest.mark.benchmark(**options('flat'))
    def test_flat(self, benchmark):
        d = dictset(self.sample_d)
        benchmark(d.flat)

    @pytest.mark.benchmark(**options('unflat'))
    def test_unflat(self, benchmark):
        d = dictset(self.sample_d).flat()
        benchmark(d.unflat)

    @pytest.mark.benchmark(**options('extract'))
    def test_extract(self, benchmark):
        d = dictset(self.sample_d)
        r = benchmark(d.extract, ['a', 'b'])

    @pytest.mark.benchmark(**options('extract'))
    def test_extract_invalid_key(self, benchmark):
        d = dictset(self.sample_d)
        # d.ii is valid, it's a very slow lookup, and it doesn't return
        r = benchmark(d.extract, ['a', 'b', 'd.ii'])

    @pytest.mark.benchmark(**options('subset'))
    def test_subset(self, benchmark):
        d = dictset(self.sample_d)
        r = benchmark(d.subset, ['a', 'b', 'd'])

    @pytest.mark.benchmark(**options('update_with'))
    def test_update_with(self, benchmark):
        # Include d in both to have a collision
        d = dictset(self.sample_d).subset(['a', 'b', 'd'])
        e = dictset(self.sample_d).subset(['c', 'd', 'e'])
        r = benchmark(d.update_with, e)
