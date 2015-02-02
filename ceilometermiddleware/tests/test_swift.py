#
# Copyright 2012 eNovance <licensing@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import mock
from oslo_config import cfg
from oslo_utils import timeutils
import six

from ceilometermiddleware import swift
from ceilometermiddleware.tests import base as tests_base


class FakeApp(object):
    def __init__(self, body=None):
        self.body = body or ['This string is 28 bytes long']

    def __call__(self, env, start_response):
        yield
        start_response('200 OK', [
            ('Content-Type', 'text/plain'),
            ('Content-Length', str(sum(map(len, self.body))))
        ])
        while env['wsgi.input'].read(5):
            pass
        for line in self.body:
            yield line


class FakeRequest(object):
    """A bare bones request object

    The middleware will inspect this for request method,
    wsgi.input and headers.
    """

    def __init__(self, path, environ=None, headers=None):
        environ = environ or {}
        headers = headers or {}

        environ['PATH_INFO'] = path

        if 'wsgi.input' not in environ:
            environ['wsgi.input'] = six.moves.cStringIO('')

        for header, value in six.iteritems(headers):
            environ['HTTP_%s' % header.upper()] = value
        self.environ = environ


@mock.patch('oslo.messaging.get_transport', mock.MagicMock())
class TestSwift(tests_base.TestCase):

    def setUp(self):
        super(TestSwift, self).setUp()
        cfg.CONF([], project='ceilometermiddleware')
        self.addCleanup(cfg.CONF.reset)

    @staticmethod
    def start_response(*args):
            pass

    def test_get(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/1.0/account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            resp = app(req.environ, self.start_response)
            self.assertEqual(["This string is 28 bytes long"], list(resp))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertEqual(28, data[2]['measurements'][0]['result'])
            self.assertEqual('storage.objects.outgoing.bytes',
                             data[2]['measurements'][0]['metric']['name'])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])

    def test_put(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = FakeRequest(
            '/1.0/account/container/obj',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input':
                     six.moves.cStringIO('some stuff')})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertEqual(10, data[2]['measurements'][0]['result'])
            self.assertEqual('storage.objects.incoming.bytes',
                             data[2]['measurements'][0]['metric']['name'])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('put', data[2]['target']['action'])

    def test_post(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = FakeRequest(
            '/1.0/account/container/obj',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': six.moves.cStringIO('some other stuff')})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertEqual(16, data[2]['measurements'][0]['result'])
            self.assertEqual('storage.objects.incoming.bytes',
                             data[2]['measurements'][0]['metric']['name'])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('post', data[2]['target']['action'])

    def test_head(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = FakeRequest('/1.0/account/container/obj',
                          environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertIsNone(data[2].get('measurements'))
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('head', data[2]['target']['action'])

    def test_bogus_request(self):
        """Test even for arbitrary request method, this will still work."""
        app = swift.Swift(FakeApp(body=['']), {})
        req = FakeRequest('/1.0/account/container/obj',
                          environ={'REQUEST_METHOD': 'BOGUS'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertIsNone(data[2].get('measurements'))
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('bogus', data[2]['target']['action'])

    def test_get_container(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            self.assertEqual(28, data[2]['measurements'][0]['result'])
            self.assertEqual('storage.objects.outgoing.bytes',
                             data[2]['measurements'][0]['metric']['name'])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])

    def test_no_metadata_headers(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])
            http_headers = [k for k in metadata.keys()
                            if k.startswith('http_header_')]
            self.assertEqual(0, len(http_headers))

    def test_metadata_headers(self):
        app = swift.Swift(FakeApp(), {
            'metadata_headers': 'X_VAR1, x-var2, x-var3, token'
        })
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'},
                          headers={'X_VAR1': 'value1',
                                   'X_VAR2': 'value2',
                                   'TOKEN': 'token'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])
            http_headers = [k for k in metadata.keys()
                            if k.startswith('http_header_')]
            self.assertEqual(3, len(http_headers))
            self.assertEqual('value1', metadata['http_header_x_var1'])
            self.assertEqual('value2', metadata['http_header_x_var2'])
            self.assertEqual('token', metadata['http_header_token'])
            self.assertFalse('http_header_x_var3' in metadata)

    def test_metadata_headers_unicode(self):
        app = swift.Swift(FakeApp(), {
            'metadata_headers': 'unicode'
        })
        uni = u'\xef\xbd\xa1\xef\xbd\xa5'
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'},
                          headers={'UNICODE': uni})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])
            http_headers = [k for k in metadata.keys()
                            if k.startswith('http_header_')]
            self.assertEqual(1, len(http_headers))
            self.assertEqual(six.text_type(uni),
                             metadata['http_header_unicode'])

    def test_metadata_headers_on_not_existing_header(self):
        app = swift.Swift(FakeApp(), {
            'metadata_headers': 'x-var3'
        })
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])
            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])
            http_headers = [k for k in metadata.keys()
                            if k.startswith('http_header_')]
            self.assertEqual(0, len(http_headers))

    def test_bogus_path(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/5.0//',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))

    def test_missing_resource_id(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/v1/', environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))

    @mock.patch.object(timeutils, 'utcnow')
    def test_emit_event_fail(self, mocked_time):
        mocked_time.side_effect = Exception("a exception")
        app = swift.Swift(FakeApp(body=["test"]), {})
        req = FakeRequest('/1.0/account/container',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            resp = list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))
            self.assertEqual(["test"], resp)

    def test_reseller_prefix(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/1.0/AUTH_account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_custom_prefix(self):
        app = swift.Swift(FakeApp(), {'reseller_prefix': 'CUSTOM_'})
        req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_invalid_reseller_prefix(self):
        # Custom reseller prefix set, but without trailing underscore
        app = swift.Swift(
            FakeApp(), {'reseller_prefix': 'CUSTOM'})
        req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo.messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])
