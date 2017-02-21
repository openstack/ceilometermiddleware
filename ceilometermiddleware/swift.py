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
    url = rabbit://me:passwd@host:5672/virtual_host
    # set messaging driver
    driver = messagingv2
    # set topic
    topic = notifications
    # skip metering of requests from listed project ids
    ignore_projects = <proj_uuid>, <proj_uuid2>
    # Whether to send events to messaging driver in a background thread
    nonblocking_notify = False
    # Queue size for sending notifications in background thread (0=unlimited).
    # New notifications will be discarded if the queue is full.
    send_queue_size = 1000
    # Logging level control
    log_level = WARNING
"""
import datetime
import functools
import logging

from oslo_config import cfg
import oslo_messaging
from oslo_utils import strutils
from pycadf import event as cadf_event
from pycadf.helper import api
from pycadf import measurement as cadf_measurement
from pycadf import metric as cadf_metric
from pycadf import resource as cadf_resource
import six
import six.moves.queue as queue
import six.moves.urllib.parse as urlparse
import threading

LOG = logging.getLogger(__name__)


def _log_and_ignore_error(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            LOG.exception('An exception occurred processing '
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

    event_queue = None
    threadLock = threading.Lock()

    def __init__(self, app, conf):
        self._app = app
        self.ignore_projects = [
            proj.strip() for proj in
            conf.get('ignore_projects', 'gnocchi').split(',')]

        oslo_messaging.set_transport_defaults(conf.get('control_exchange',
                                                       'swift'))
        self._notifier = oslo_messaging.Notifier(
            oslo_messaging.get_transport(cfg.CONF, url=conf.get('url')),
            publisher_id='ceilometermiddleware',
            driver=conf.get('driver', 'messagingv2'),
            topics=[conf.get('topic', 'notifications')])

        self.metadata_headers = [h.strip().replace('-', '_').lower()
                                 for h in conf.get(
                                     "metadata_headers",
                                     "").split(",") if h.strip()]

        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH_')
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'

        LOG.setLevel(getattr(logging, conf.get('log_level', 'WARNING')))

        # NOTE: If the background thread's send queue fills up, the event will
        #  be discarded
        #
        # For backward compatibility we default to False and therefore wait for
        #  sending to complete. This causes swift proxy to hang if the
        #  destination is unavailable.
        self.nonblocking_notify = strutils.bool_from_string(
            conf.get('nonblocking_notify', False))

        # Initialize the sending queue and thread, but only once
        if self.nonblocking_notify and Swift.event_queue is None:
            Swift.threadLock.acquire()
            if Swift.event_queue is None:
                send_queue_size = int(conf.get('send_queue_size', 1000))
                Swift.event_queue = queue.Queue(send_queue_size)
                self.start_sender_thread()
            Swift.threadLock.release()

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
        if ((env.get('HTTP_X_SERVICE_PROJECT_ID') or
                env.get('HTTP_X_PROJECT_ID') or
                env.get('HTTP_X_TENANT_ID')) in self.ignore_projects or
                env.get('swift.source') is not None):
            return

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
            path = path.replace('/', '', 1)
            version, account, remainder = path.split('/', 2)
        except ValueError:
            try:
                version, account = path.split('/', 1)
                remainder = None
            except ValueError:
                return
        try:
            if not version or not account:
                raise ValueError('Invalid path: %s' % path)
            if remainder:
                if '/' in remainder:
                    container, obj = remainder.split('/', 1)
                else:
                    container = remainder
        except ValueError:
            return

        now = datetime.datetime.utcnow().isoformat()

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
            id=account.partition(self.reseller_prefix)[2] or path)
        target.metadata = resource_metadata
        target.action = method.lower()

        # build user details
        initiator = cadf_resource.Resource(
            typeURI='service/security/account/user',
            id=env.get('HTTP_X_USER_ID'))
        initiator.project_id = (env.get('HTTP_X_PROJECT_ID') or
                                env.get('HTTP_X_TENANT_ID'))

        # build notification body
        event = cadf_event.Event(eventTime=now, outcome=outcome,
                                 action=api.convert_req_action(method),
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

        if self.nonblocking_notify:
            try:
                Swift.event_queue.put(event, False)
                if not Swift.event_sender.is_alive():
                    Swift.threadLock.acquire()
                    self.start_sender_thread()
                    Swift.threadLock.release()

            except queue.Full:
                LOG.warning('Send queue FULL: Event %s not added', event.id)
        else:
            Swift.send_notification(self._notifier, event)

    def start_sender_thread(self):
        Swift.event_sender = SendEventThread(self._notifier)
        Swift.event_sender.daemon = True
        Swift.event_sender.start()

    @staticmethod
    def send_notification(notifier, event):
        notifier.info({}, 'objectstore.http.request', event.as_dict())


class SendEventThread(threading.Thread):

    def __init__(self, notifier):
        super(SendEventThread, self).__init__()
        self.notifier = notifier

    def run(self):
        """Send events without blocking swift proxy."""
        while True:
            try:
                LOG.debug('Wait for event from send queue')
                event = Swift.event_queue.get()
                LOG.debug('Got event %s from queue - now send it', event.id)
                Swift.send_notification(self.notifier, event)
                LOG.debug('Event %s sent.', event.id)
            except BaseException:
                LOG.exception("SendEventThread loop exception")


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def filter(app):
        return Swift(app, conf)
    return filter
