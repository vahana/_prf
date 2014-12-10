#!/usr/bin/env python
# -*- coding: utf-8 -*-
# removing this encoding

from datetime import datetime, date
from decimal import Decimal
import json
import unittest

import mock


class TestRenderers(unittest.TestCase):

    def setUp(self):
        self.now = datetime.now()
        self.today = date.today()

    def _get_dummy_result(self):
        obj = {
            'integer': 1,
            'string': "hello world",
            'unicode': u"yéyé",
            'list': [1, 2, 3, 4],
            'obj': {
                'wow': {
                    'yop': 'opla'
                }
            },
            'price': Decimal('102.3'),
            'datetime': self.now,
            'date': self.today
        }
        return obj

    def _get_dummy_expected(self):
        return {
            'integer': 1,
            'string': "hello world",
            'unicode': u"yéyé",
            'list': [1, 2, 3, 4],
            'obj': {
                'wow': {
                    'yop': 'opla'
                }
            },
            'price': str(Decimal('102.3')),
            'datetime': self.now.isoformat(),
            'date': self.today.isoformat()
        }

    def test_JSONEncoder_datetime_decimal(self):
        from prf.renderers import _JSONEncoder
        obj = self._get_dummy_result()

        res_dumps = json.dumps(obj, cls=_JSONEncoder)
        self.assertDictEqual(self._get_dummy_expected(), json.loads(res_dumps))

        enc = _JSONEncoder()

        self.assertEqual(self.now.isoformat(), enc.default(self.now))

        self.assertRaises(TypeError, enc.default, {})

    def test_JsonRendererFactory(self):
        from  prf.renderers import JsonRendererFactory

        request = mock.MagicMock()
        request.response.default_content_type = 'text/html'
        request.response.content_type = 'text/html'
        request.url = 'http://'

        jfr = JsonRendererFactory({
            'name': 'json',
            'package': None,
            'registry': None
        })

        result = json.loads(jfr(self._get_dummy_result(), {'request': request}))
        self.assertDictContainsSubset(self._get_dummy_expected(), result)
        self.assertEqual('application/json', request.response.content_type)
