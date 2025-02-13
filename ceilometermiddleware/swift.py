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
    ignore_projects = <proj_uuid>, <proj_uuid2>, <proj_name>
    # Whether to send events to messaging driver in a background thread
    nonblocking_notify = False
    # Queue size for sending notifications in background thread (0=unlimited).
    # New notifications will be discarded if the queue is full.
    send_queue_size = 1000
    # Logging level control
    log_level = WARNING

    # All keystoneauth1 options can be set to query project name for
    # ignore_projects option, here is just a example:
    auth_type = password
    auth_url = https://[::1]:5000
    project_name = service
    project_domain_name = Default
    username = user
    user_domain_name = Default
    password = a_big_secret
    interface = public
"""
import datetime
import functools
import logging

from keystoneauth1 import exceptions as ksa_exc
from keystoneauth1.loading import adapter as ksa_adapter
from keystoneauth1.loading import base as ksa_base
from keystoneauth1.loading import session as ksa_session
from keystoneclient.v3 import client as ks_client
from oslo_config import cfg
import oslo_messaging
from oslo_utils import strutils
from pycadf import event as cadf_event
from pycadf.helper import api
from pycadf import measurement as cadf_measurement
from pycadf import metric as cadf_metric
from pycadf import resource as cadf_resource
import queue
import threading
import urllib.parse as urlparse

LOG = logging.getLogger(__name__)


def list_from_csv(comma_separated_str):
    if comma_separated_str:
        return list(
            filter(lambda x: x,
                   map(lambda x: x.strip(),
                       comma_separated_str.split(','))))
    return []


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

    def close(self):
        close_method = getattr(self.wsgi_input, 'close', None)
        if callable(close_method):
            close_method()


class KeystoneClientLoader(ksa_adapter.Adapter):
    """Keystone client adapter loader.

    Keystone client and Keystoneauth1 adapter take exactly the same options, so
    it's safe to create a keystone client with keystoneauth adapter options.
    """

    @property
    def plugin_class(self):
        return ks_client.Client


class Swift(object):
    """Swift middleware used for counting requests."""

    event_queue = None
    threadLock = threading.Lock()

    DEFAULT_IGNORE_PROJECT_NAMES = ['service']

    def __init__(self, app, conf):
        self._app = app

        self.ignore_projects = self._get_ignore_projects(conf)

        extra_config_files = conf.get('extra_config_files')
        if extra_config_files is not None:
            extra_config_files = list_from_csv(extra_config_files)

        extra_config_dirs = conf.get('extra_config_dirs')
        if extra_config_dirs is not None:
            extra_config_dirs = list_from_csv(extra_config_dirs)

        oslo_conf = cfg.ConfigOpts()
        oslo_conf([], project='swift',
                  default_config_files=extra_config_files,
                  default_config_dirs=extra_config_dirs,
                  validate_default_values=True)

        oslo_messaging.set_transport_defaults(conf.get('control_exchange',
                                                       'swift'))
        self._notifier = oslo_messaging.Notifier(
            oslo_messaging.get_notification_transport(oslo_conf,
                                                      url=conf.get('url')),
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

    def _get_ignore_projects(self, conf):
        if 'auth_type' not in conf:
            LOG.info("'auth_type' is not set assuming ignore_projects are "
                     "only project uuid.")
            return list_from_csv(conf.get('ignore_projects'))

        if 'ignore_projects' in conf:
            ignore_projects = list_from_csv(conf.get('ignore_projects'))
        else:
            ignore_projects = self.DEFAULT_IGNORE_PROJECT_NAMES

        if not ignore_projects:
            return []

        def opt_getter(opt):
            # TODO(sileht): This method does not support deprecated opt names
            val = conf.get(opt.name)
            if val is None:
                val = conf.get(opt.dest)
            return val

        auth_type = conf.get('auth_type')
        plugin = ksa_base.get_plugin_loader(auth_type)

        auth = plugin.load_from_options_getter(opt_getter)
        session = ksa_session.Session().load_from_options_getter(
            opt_getter, auth=auth)
        client = KeystoneClientLoader().load_from_options_getter(
            opt_getter, session=session)

        projects = []
        for name_or_id in ignore_projects:
            projects.extend(self._get_keystone_projects(client, name_or_id))
        return projects

    @staticmethod
    def _get_keystone_projects(client, name_or_id):
        try:
            return [client.projects.get(name_or_id)]
        except ksa_exc.NotFound:
            pass
        if isinstance(name_or_id, bytes):
            name_or_id = name_or_id.decode('utf-8', 'strict')
        projects = client.projects.list(name=name_or_id)
        if not projects:
            LOG.warning("fail to find project '%s' in keystone", name_or_id)
        return [p.id for p in projects]

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
                    try:
                        chunk = next(iterator)
                    except StopIteration:
                        chunk = ''
            finally:
                close_method = getattr(iterable, 'close', None)
                if callable(close_method):
                    close_method()
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
        if (
                (env.get('HTTP_X_SERVICE_PROJECT_ID')
                 or env.get('HTTP_X_PROJECT_ID')
                 or env.get('HTTP_X_TENANT_ID')) in self.ignore_projects
                or env.get('swift.source') is not None):
            return

        path = urlparse.quote(env.get('swift.backend_path', env['PATH_INFO']))
        method = env['REQUEST_METHOD']
        headers = {}
        for header in env:
            if header.startswith('HTTP_') and env[header]:
                key = header[5:]
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
        if self.reseller_prefix:
            resource_id = account.partition(self.reseller_prefix)[2] or path
        else:
            resource_id = account
        target = cadf_resource.Resource(
            typeURI='service/storage/object',
            id=resource_id)
        target.metadata = resource_metadata
        target.action = method.lower()

        # build user details
        initiator = cadf_resource.Resource(
            typeURI='service/security/account/user',
            id=env.get('HTTP_X_USER_ID'))
        initiator.project_id = (env.get('HTTP_X_PROJECT_ID')
                                or env.get('HTTP_X_TENANT_ID'))

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
