#
# Copyright 2012 eNovance <licensing@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Telemetry Middleware for Swift Proxy

Configuration:
In /etc/swift/proxy-server.conf on the main pipeline add "ceilometer" just
before "proxy-server" and add the following filter in the file:
.. code-block:: python
    [filter:ceilometer]
    paste.filter_factory = ceilometermiddleware.swift:filter_factory
    # Some optional configuration this allow to publish additional metadata
    metadata_headers = X-TEST
    # Set reseller prefix (defaults to "AUTH_" if not set)
    reseller_prefix = AUTH_
    # Set control_exchange to publish to.
    control_exchange = swift
    # Set transport url
    transport_url = rabbit://me:passwd@host:5672/virtual_host
    # set messaging driver
    driver = messaging
    # set topic
    topic = notifications
"""
import functools
import logging

import oslo.messaging
from oslo_config import cfg
from oslo_context import context
from oslo_utils import timeutils
from pycadf import event as cadf_event
from pycadf import measurement as cadf_measurement
from pycadf import metric as cadf_metric
from pycadf import resource as cadf_resource
import six
import six.moves.urllib.parse as urlparse

_LOG = logging.getLogger(__name__)


def _log_and_ignore_error(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            _LOG.exception('An exception occurred processing '
                           'the API call: %s ', e)
    return wrapper


class InputProxy(object):
    """File-like object that counts bytes read.

    To be swapped in for wsgi.input for accounting purposes.
    Borrowed from swift.common.utils. Duplicated here to avoid
    dependency on swift package.
    """
    def __init__(self, wsgi_input):
        self.wsgi_input = wsgi_input
        self.bytes_received = 0

    def read(self, *args, **kwargs):
        """Pass read request to the underlying file-like object

        Add bytes read to total.
        """
        chunk = self.wsgi_input.read(*args, **kwargs)
        self.bytes_received += len(chunk)
        return chunk

    def readline(self, *args, **kwargs):
        """Pass readline request to the underlying file-like object

        Add bytes read to total.
        """
        line = self.wsgi_input.readline(*args, **kwargs)
        self.bytes_received += len(line)
        return line


class Swift(object):
    """Swift middleware used for counting requests."""

    def __init__(self, app, conf):
        self._app = app

        oslo.messaging.set_transport_defaults(conf.get('control_exchange',
                                                       'swift'))
        self._notifier = oslo.messaging.Notifier(
            oslo.messaging.get_transport(cfg.CONF, url=conf.get('url')),
            publisher_id='ceilometermiddleware',
            driver=conf.get('driver', 'messaging'),
            topic=conf.get('topic', 'notifications'))

        self.metadata_headers = [h.strip().replace('-', '_').lower()
                                 for h in conf.get(
                                     "metadata_headers",
                                     "").split(",") if h.strip()]

        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH_')
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'

    def __call__(self, env, start_response):
        start_response_args = [None]
        input_proxy = InputProxy(env['wsgi.input'])
        env['wsgi.input'] = input_proxy

        def my_start_response(status, headers, exc_info=None):
            start_response_args[0] = (status, list(headers), exc_info)

        def iter_response(iterable):
            iterator = iter(iterable)
            try:
                chunk = next(iterator)
                while not chunk:
                    chunk = next(iterator)
            except StopIteration:
                chunk = ''

            if start_response_args[0]:
                start_response(*start_response_args[0])
            bytes_sent = 0
            try:
                while chunk:
                    bytes_sent += len(chunk)
                    yield chunk
                    chunk = next(iterator)
            finally:
                self.emit_event(env, input_proxy.bytes_received, bytes_sent)

        try:
            iterable = self._app(env, my_start_response)
        except Exception:
            self.emit_event(env, input_proxy.bytes_received, 0, 'failure')
            raise
        else:
            return iter_response(iterable)

    @_log_and_ignore_error
    def emit_event(self, env, bytes_received, bytes_sent, outcome='success'):
        path = urlparse.quote(env['PATH_INFO'])
        method = env['REQUEST_METHOD']
        headers = {}
        for header in env:
            if header.startswith('HTTP_') and env[header]:
                key = header[5:]
                if isinstance(env[header], six.text_type):
                    headers[key] = six.text_type(env[header])
                else:
                    headers[key] = str(env[header])

        try:
            container = obj = None
            version, account, remainder = path.replace(
                '/', '', 1).split('/', 2)
            if not version or not account:
                raise ValueError('Invalid path: %s' % path)
            if remainder:
                if '/' in remainder:
                    container, obj = remainder.split('/', 1)
                else:
                    container = remainder
        except ValueError:
            return

        now = timeutils.utcnow().isoformat()

        resource_metadata = {
            "path": path,
            "version": version,
            "container": container,
            "object": obj,
        }

        for header in self.metadata_headers:
            if header.upper() in headers:
                resource_metadata['http_header_%s' % header] = headers.get(
                    header.upper())

        # build object store details
        target = cadf_resource.Resource(
            typeURI='service/storage/object',
            id=account.partition(self.reseller_prefix)[2])
        target.metadata = resource_metadata
        target.action = method.lower()

        # build user details
        initiator = cadf_resource.Resource(
            typeURI='service/security/account/user',
            id=env.get('HTTP_X_USER_ID'))
        initiator.project_id = env.get('HTTP_X_TENANT_ID')

        # build notification body
        event = cadf_event.Event(eventTime=now, outcome=outcome,
                                 initiator=initiator, target=target,
                                 observer=cadf_resource.Resource(id='target'))

        # measurements
        if bytes_received:
            event.add_measurement(cadf_measurement.Measurement(
                result=bytes_received,
                metric=cadf_metric.Metric(
                    name='storage.objects.incoming.bytes', unit='B')))
        if bytes_sent:
            event.add_measurement(cadf_measurement.Measurement(
                result=bytes_sent,
                metric=cadf_metric.Metric(
                    name='storage.objects.outgoing.bytes', unit='B')))

        self._notifier.info(context.get_admin_context().to_dict(),
                            'objectstore.http.request', event.as_dict())


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def filter(app):
        return Swift(app, conf)
    return filter
