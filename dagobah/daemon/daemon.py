""" HTTP Daemon implementation for Dagobah service. """

import os
import logging

from flask import Flask, send_from_directory
import yaml

from dagobah.core import Dagobah, EventHandler
from dagobah.email import get_email_handler
from dagobah.backend.base import BaseBackend
from dagobah.backend.mongo import MongoBackend

app = Flask(__name__)
APP_PORT = 9000


def init_dagobah(testing=False):

    location = os.path.realpath(os.path.join(os.getcwd(),
                                             os.path.dirname(__file__)))

    config_file = open(os.path.join(location, 'dagobahd.yaml'))
    config = yaml.load(config_file.read())
    config_file.close()

    init_logger(location, config)

    backend = get_backend(config)
    event_handler = configure_event_hooks(config)
    dagobah = Dagobah(backend, event_handler)

    known_ids = [id for id in backend.get_known_dagobah_ids()
                 if id != dagobah.dagobah_id]
    if len(known_ids) > 1:
        # need a way to handle this intelligently through config
        raise ValueError('could not infer dagobah ID, ' +
                         'multiple available in backend')

    if known_ids:
        dagobah.from_backend(known_ids[0])

    return dagobah


def configure_event_hooks(config):
    """ Returns an EventHandler instance with registered hooks. """

    def print_event_info(**kwargs):
        print kwargs.get('event_params', {})

    def job_complete_email(email_handler, **kwargs):
        email_handler.send_job_completed(kwargs['event_params'])

    def job_failed_email(email_handler, **kwargs):
        email_handler.send_job_failed(kwargs['event_params'])

    def task_failed_email(email_handler, **kwargs):
        email_handler.send_task_failed(kwargs['event_params'])

    handler = EventHandler()

    email_handler = get_email_handler(config['Dagobahd'].get('email', None),
                                      config['Email'])
    handler.register('job_complete', print_event_info)
    handler.register('job_complete', job_complete_email, email_handler)

    handler.register('job_failed', job_failed_email, email_handler)

    handler.register('task_failed', task_failed_email, email_handler)

    return handler


def init_logger(location, config):
    """ Initialize the logger with settings from config. """

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    if config['Logging'].get('enabled', False) == False:
        handler = NullHandler()
        logging.getLogger("dagobah").addHandler(handler)
        return

    if config['Logging'].get('logfile', 'default') == 'default':
        path = os.path.join(location, 'dagobah.log')
    else:
        path = config['Logging']['logfile']

    level_string = config['Logging'].get('loglevel', 'info').upper()
    numeric_level = getattr(logging, level_string, None)

    logging.basicConfig(filename=path, level=numeric_level)

    logging.info('Logger initialized at level %s' % level_string)


def get_backend(config):
    """ Returns a backend instance based on the Daemon config file. """

    backend_string = config['Dagobahd']['backend']

    if backend_string.lower() == 'none':
        return BaseBackend()

    elif backend_string.lower() == 'mongo':

        backend_kwargs = {}
        for conf_kwarg in ['host', 'port', 'db',
                           'dagobah_collection', 'job_collection',
                           'log_collection']:
            backend_kwargs[conf_kwarg] = config['MongoBackend'][conf_kwarg]

        backend_kwargs['port'] = int(backend_kwargs['port'])
        return MongoBackend(**backend_kwargs)

    raise ValueError('unknown backend type specified in conf')



@app.route('/favicon.ico')
def favicon_redirect():
    return send_from_directory(os.path.join(app.root_path,
                                            'static', 'img'),
                               'favicon.ico',
                               mimetype='image/vnd.microsoft.icon')


dagobah = init_dagobah()
app.config['dagobah'] = dagobah

from dagobah.daemon.views import *
from dagobah.daemon.api import *

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=APP_PORT)
