import unittest
import mock

class TestPyramidIntegration(unittest.TestCase):

    def test_includeme(self):
        from prf.resource import includeme, add_resource
        from prf.renderers import JsonRendererFactory

        config = mock.Mock()
        includeme(config)

        self.assertEqual(1, config.add_directive.call_count)
        self.assertEqual(2, config.add_renderer.call_count)
        # config.add_renderer.assert_called_with('json', JsonRendererFactory)
