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
from io import StringIO
import threading
import unittest
from unittest import mock

from oslo_config import cfg

from ceilometermiddleware import swift
from ceilometermiddleware.tests import base as tests_base
from keystoneauth1.fixture import keystoneauth_betamax as betamax


class FakeApp:
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
        yield from self.body


class FakeRequest:
    """A bare bones request object

    The middleware will inspect this for request method,
    wsgi.input and headers.
    """

    def __init__(self, path, environ=None, headers=None):
        environ = environ or {}
        headers = headers or {}

        environ['PATH_INFO'] = path

        if 'wsgi.input' not in environ:
            environ['wsgi.input'] = StringIO('')

        for header, value in headers.items():
            environ['HTTP_%s' % header.upper()] = value
        self.environ = environ


@mock.patch('oslo_messaging.get_transport', mock.MagicMock())
class TestSwift(tests_base.TestCase):

    def setUp(self):
        super().setUp()
        cfg.CONF([], project='ceilometermiddleware')
        self.addCleanup(cfg.CONF.reset)

    @staticmethod
    def start_response(*args):
        pass

    def get_request(self, path, environ=None, headers=None):
        return FakeRequest(path, environ=environ, headers=headers)

    # ---- helpers for new storage.api.request metric ----

    @staticmethod
    def _measurements(payload):
        return payload.get('measurements') or []

    def _find_measurement(self, payload, metric_name):
        for m in self._measurements(payload):
            if m.get('metric', {}).get('name') == metric_name:
                return m
        self.fail(
            "Missing measurement '{}'. Got: {}".format(
                metric_name,
                [
                    mm.get('metric', {}).get('name')
                    for mm in self._measurements(payload)
                ]
            )
        )

    def _assert_has_api_request(self, payload, result=1):
        m = self._find_measurement(payload, 'storage.api.request')
        self.assertEqual(result, m.get('result'))

    def _assert_no_bytes_metrics(self, payload):
        byte_metrics = {
            'storage.objects.incoming.bytes',
            'storage.objects.outgoing.bytes',
        }
        self.assertFalse(
            any(m.get('metric', {}).get('name') in byte_metrics
                for m in self._measurements(payload))
        )

    # ---- tests ----

    def test_get(self):
        app = swift.Swift(FakeApp(), {})
        req = self.get_request('/1.0/account/container/obj',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            resp = app(req.environ, self.start_response)
            self.assertEqual(["This string is 28 bytes long"], list(resp))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            bytes_m = self._find_measurement(
                data[2],
                'storage.objects.outgoing.bytes',
            )
            self.assertEqual(28, bytes_m['result'])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])

    def test_get_background(self):
        notified = threading.Event()
        app = swift.Swift(FakeApp(),
                          {"nonblocking_notify": "True",
                           "send_queue_size": "1"})
        req = self.get_request('/1.0/account/container/obj',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info',
                        side_effect=lambda *args, **kwargs: notified.set()
                        ) as notify:
            resp = app(req.environ, self.start_response)
            self.assertEqual(["This string is 28 bytes long"], list(resp))
            notified.wait()
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            bytes_m = self._find_measurement(
                data[2],
                'storage.objects.outgoing.bytes',
            )
            self.assertEqual(28, bytes_m['result'])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])

    def test_put(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = self.get_request(
            '/1.0/account/container/obj',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input':
                     StringIO('some stuff')})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            bytes_m = self._find_measurement(
                data[2],
                'storage.objects.incoming.bytes',
            )
            self.assertEqual(10, bytes_m['result'])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('put', data[2]['target']['action'])

    def test_post(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = self.get_request(
            '/1.0/account/container/obj',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': StringIO('some other stuff')})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            bytes_m = self._find_measurement(
                data[2],
                'storage.objects.incoming.bytes',
            )
            self.assertEqual(16, bytes_m['result'])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('post', data[2]['target']['action'])

    def test_head(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = self.get_request('/1.0/account/container/obj',
                               environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            # request metric present, no bytes metrics
            self._assert_has_api_request(data[2], result=1)
            self._assert_no_bytes_metrics(data[2])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('head', data[2]['target']['action'])

    def test_bogus_request(self):
        """Test even for arbitrary request method, this will still work."""
        app = swift.Swift(FakeApp(body=['']), {})
        req = self.get_request('/1.0/account/container/obj',
                               environ={'REQUEST_METHOD': 'BOGUS'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            # Bogus methods still count as an API request
            self._assert_has_api_request(data[2], result=1)
            self._assert_no_bytes_metrics(data[2])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertEqual('obj', metadata['object'])
            self.assertEqual('bogus', data[2]['target']['action'])

    def test_get_container(self):
        app = swift.Swift(FakeApp(), {})
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            bytes_m = self._find_measurement(
                data[2],
                'storage.objects.outgoing.bytes',
            )
            self.assertEqual(28, bytes_m['result'])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])

    def test_no_metadata_headers(self):
        app = swift.Swift(FakeApp(), {})
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

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
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={'X_VAR1': 'value1',
                                        'X_VAR2': 'value2',
                                        'TOKEN': 'token'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

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
            self.assertNotIn('http_header_x_var3', metadata)

    def test_metadata_headers_unicode(self):
        app = swift.Swift(FakeApp(), {
            'metadata_headers': 'unicode'
        })
        uni = '\xef\xbd\xa1\xef\xbd\xa5'
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={'UNICODE': uni})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertEqual('container', metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('get', data[2]['target']['action'])
            http_headers = [k for k in metadata.keys()
                            if k.startswith('http_header_')]
            self.assertEqual(1, len(http_headers))
            self.assertEqual(str(uni),
                             metadata['http_header_unicode'])

    def test_metadata_headers_on_not_existing_header(self):
        app = swift.Swift(FakeApp(), {
            'metadata_headers': 'x-var3'
        })
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)

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
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))

    def test_missing_resource_id(self):
        app = swift.Swift(FakeApp(), {})
        req = FakeRequest('/v1/', environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))

    @mock.patch('urllib.parse.quote')
    def test_emit_event_fail(self, mocked_func):
        mocked_func.side_effect = Exception("a exception")
        app = swift.Swift(FakeApp(body=["test"]), {})
        req = self.get_request('/1.0/account/container',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            resp = list(app(req.environ, self.start_response))
            self.assertEqual(0, len(notify.call_args_list))
            self.assertEqual(["test"], resp)

    def test_reseller_prefix(self):
        app = swift.Swift(FakeApp(), {})
        req = self.get_request('/1.0/AUTH_account/container/obj',
                               environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_custom_reseller_prefix(self):
        app = swift.Swift(FakeApp(), {'reseller_prefix': 'CUSTOM_'})
        req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_empty_reseller_prefix(self):
        app = swift.Swift(FakeApp(), {'reseller_prefix': ''})
        req = FakeRequest('/1.0/account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_incomplete_reseller_prefix(self):
        # Custom reseller prefix set, but without trailing underscore
        app = swift.Swift(
            FakeApp(), {'reseller_prefix': 'CUSTOM'})
        req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("account", data[2]['target']['id'])

    def test_invalid_reseller_prefix(self):
        app = swift.Swift(
            FakeApp(), {'reseller_prefix': 'AUTH_'})
        req = FakeRequest('/1.0/admin/bucket',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual("1.0/admin/bucket", data[2]['target']['id'])

    def test_ignore_requests_from_project(self):
        app = swift.Swift(FakeApp(), {'ignore_projects': 'skip_proj'})

        for proj_attr in ['HTTP_X_SERVICE_PROJECT_ID', 'HTTP_X_PROJECT_ID',
                          'HTTP_X_TENANT_ID']:
            for proj, calls in [('good', 1), ('skip_proj', 0)]:
                req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                                  environ={'REQUEST_METHOD': 'GET',
                                           proj_attr: proj})
                with mock.patch('oslo_messaging.Notifier.info') as notify:
                    list(app(req.environ, self.start_response))
                    self.assertEqual(calls, len(notify.call_args_list))

    def test_ignore_requests_from_multiple_projects(self):
        app = swift.Swift(FakeApp(), {'ignore_projects': 'skip_proj, ignore'})

        for proj_attr in ['HTTP_X_SERVICE_PROJECT_ID', 'HTTP_X_PROJECT_ID',
                          'HTTP_X_TENANT_ID']:
            for proj, calls in [('good', 1), ('skip_proj', 0),
                                ('also_good', 1), ('ignore', 0)]:
                req = FakeRequest('/1.0/CUSTOM_account/container/obj',
                                  environ={'REQUEST_METHOD': 'GET',
                                           proj_attr: proj})
                with mock.patch('oslo_messaging.Notifier.info') as notify:
                    list(app(req.environ, self.start_response))
                    self.assertEqual(calls, len(notify.call_args_list))

    def test_only_reseller_prefix(self):
        app = swift.Swift(
            FakeApp(), {'reseller_prefix': 'CUSTOM'})
        req = FakeRequest('/1.0/CUSTOM/container/obj',
                          environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            data = notify.call_args_list[0][0]
            self.assertIsNot(0, len(data[2]['target']['id']))

    def test_head_account(self):
        app = swift.Swift(FakeApp(body=['']), {})
        req = FakeRequest('/1.0/account',
                          environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertEqual(1, len(notify.call_args_list))
            data = notify.call_args_list[0][0]
            self.assertEqual('objectstore.http.request', data[1])

            self._assert_has_api_request(data[2], result=1)
            self._assert_no_bytes_metrics(data[2])

            metadata = data[2]['target']['metadata']
            self.assertEqual('1.0', metadata['version'])
            self.assertIsNone(metadata['container'])
            self.assertIsNone(metadata['object'])
            self.assertEqual('head', data[2]['target']['action'])

    def test_put_with_swift_source(self):
        app = swift.Swift(FakeApp(), {})

        req = FakeRequest(
            '/1.0/account/container/obj',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input':
                     StringIO('some stuff'),
                     'swift.source': 'RL'})
        with mock.patch('oslo_messaging.Notifier.info') as notify:
            list(app(req.environ, self.start_response))
            self.assertFalse(notify.called)

    def test_ignore_projects_without_keystone(self):
        app = swift.Swift(FakeApp(), {
            'ignore_projects': 'cf0356aaac7c42bba5a744339a6169fa,'
                               '18157dd635bb413c9e27686fee93c583',
        })
        self.assertEqual(["cf0356aaac7c42bba5a744339a6169fa",
                          "18157dd635bb413c9e27686fee93c583"],
                         app.ignore_projects)

    @unittest.skip("fixme: needs to add missing mock coverage")
    @mock.patch.object(swift.LOG, 'warning')
    def test_ignore_projects_with_keystone(self, warning):
        self.useFixture(betamax.BetamaxFixture(
            cassette_name='list_projects',
            cassette_library_dir='ceilometermiddleware/tests/data',
        ))
        app = swift.Swift(FakeApp(), {
            'auth_type': 'v2password',
            'auth_url': 'https://[::1]:5000/v2.0',
            'username': 'admin',
            'tenant_name': 'admin',
            'password': 'secret',
            'ignore_projects': 'service,gnocchi',
        })
        self.assertEqual(["147cc0a9263c4964926f3ee7b6ba3685"],
                         app.ignore_projects)
        warning.assert_called_once_with(
            "fail to find project '%s' in keystone", "gnocchi")


class TestSwiftS3Api(TestSwift):

    def get_request(self, path, environ=None, headers=None):
        environ = environ or {}
        # Add Swift Path in environ, provided by swift s3api middleware
        environ['swift.backend_path'] = path
        # Emulate S3 api PATH_INFO by removing /v1 and account parts
        path = '/' + path.split('/', 3)[-1]

        return FakeRequest(path, environ=environ, headers=headers)
