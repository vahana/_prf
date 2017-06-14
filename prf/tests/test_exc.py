import mock
import json
import pytest
import prf.exc


def fake_resp(code):
    return mock.MagicMock(
        code = str(code),
        status_code = code,
        headers = [],
        title = 'title',
        detail = 'detail',
        explanation = 'explanation'
    )


class TestExc(object):
    @mock.patch('prf.exc.log_exception')
    def test_create_response(self, fake_log_exception):
        out = prf.exc.create_response(fake_resp(200), {})
        assert fake_log_exception.call_count == 0
        # Temporarily pop timestamp, we need to freeze time to test it
        d = json.loads(out.body)
        d.pop('timestamp')

        assert {
            'explanation': 'explanation',
            'code': '200',
            'detail': 'detail',
            'title': 'title'
        } == d

        assert out.content_type == 'application/json'

    @mock.patch('prf.exc.add_stack')
    @mock.patch('prf.exc.logger')
    def test_log_exception(self, fake_logger, fake_add_stack):
        request = mock.MagicMock(
            url = 'url',
            remote_user = 'remote_user',
            client_addr = 'client_addr',
            remote_addr = 'remote_addr'
        )

        out = prf.exc.log_exception(fake_resp(400),
                params = dict(
                    headers = ['Header'],
                    request = request,
                    extra = {'a': 123},
                    detail = 'param detail'
                )
            )

        assert fake_add_stack.called
        assert fake_logger.error.called

    @mock.patch('prf.exc.log_exception')
    def test_create_response_w_log(self, fake_log_exception):

        in_resp = mock.MagicMock()
        in_resp.code = '400'
        in_resp.status_code = 400
        out = prf.exc.create_response(in_resp, {})

        assert 'error_id' in json.loads(out.body)
        assert fake_log_exception.call_count == 1

    def test_exception_response(self):
        out = prf.exc.exception_response(200)
        assert out.code == 200
        assert out.content_type == 'application/json'
        assert 'error_id' not in out.json

        out = prf.exc.exception_response(400, extra={'a':123})
        assert 'error_id' in out.json

    def test_statuses(self):
        res = {'id':1}
        out = prf.exc.HTTPCreated(
            location = 'http://location',
            resource = res
        )
        assert out.json['resource'] == {'self': 'http://location', 'id': 1}
