""" HTTP Daemon implementation for Dagobah service. """

import os
import sys
import logging

from flask import Flask, send_from_directory
from flask_login import LoginManager
import yaml

from .. import return_standard_conf
from ..core import Dagobah, EventHandler
from ..email import get_email_handler

app = Flask(__name__)

login_manager = LoginManager()
login_manager.login_view = "login"

location = os.path.realpath(os.path.join(os.getcwd(),
                                         os.path.dirname(__file__)))

def replace_nones(dict_or_list):
    """Update a dict or list in place to replace
    'none' string values with Python None."""

    def replace_none_in_value(value):
        if isinstance(value, basestring) and value.lower() == "none":
            return None
        return value

    items = dict_or_list.iteritems() if isinstance(dict_or_list, dict) else enumerate(dict_or_list)

    for accessor, value in items:
        if isinstance(value, (dict, list)):
            replace_nones(value)
        else:
            dict_or_list[accessor] = replace_none_in_value(value)

def get_config_file():
    """ Return the loaded config file if one exists. """

    # config will be created here if we can't find one
    new_config_path = os.path.expanduser('~/.dagobahd.yml')

    config_dirs = ['/etc',
                   os.path.expanduser('~')]
    config_filenames = ['dagobahd.yml',
                        'dagobahd.yaml',
                        '.dagobahd.yml',
                        '.dagobahd.yaml']

    for directory in config_dirs:
        for filename in config_filenames:
            try:
                if os.path.isfile(os.path.join(directory, filename)):
                    to_load = open(os.path.join(directory, filename))
                    config = yaml.load(to_load.read())
                    to_load.close()
                    replace_nones(config)
                    return config
            except:
                pass

    # if we made it to here, need to create a config file
    # double up on notifications here to make sure first-time user sees it
    print 'Creating new config file in home directory'
    logging.info('Creating new config file in home directory')
    new_config = open(new_config_path, 'w')
    new_config.write(return_standard_conf())
    new_config.close()

    new_config = open(new_config_path, 'r')
    config = yaml.load(new_config.read())
    new_config.close()
    replace_nones(config)
    return config


def configure_app():

    app.secret_key = get_conf(config, 'Dagobahd.app_secret', 'default_secret')
    app.config['LOGIN_DISABLED'] = get_conf(config,
                                            'Dagobahd.auth_disabled',
                                            False)
    app.config['APP_PASSWORD'] = get_conf(config,
                                          'Dagobahd.password', 'dagobah')

    app.config['AUTH_RATE_LIMIT'] = 30
    app.config['AUTH_ATTEMPTS'] = []
    app.config['APP_HOST'] = get_conf(config, 'Dagobahd.host', '127.0.0.1')
    app.config['APP_PORT'] = get_conf(config, 'Dagobahd.port', '9000')

    login_manager.init_app(app)


def get_conf(config, path, default=None):
    current = config
    for level in path.split('.'):
        if level not in current:
            msg = 'Defaulting missing config key %s to %s' % (path, default)
            print msg
            logging.warning(msg)
            return default
        current = current[level]
    return current


def init_dagobah(testing=False):

    init_logger(location, config)

    backend = get_backend(config)
    event_handler = configure_event_hooks(config)
    ssh_config = get_conf(config, 'Dagobahd.ssh_config', '~/.ssh/config')

    if not os.path.isfile(os.path.expanduser(ssh_config)):
        logging.warn("SSH config doesn't exist, no remote hosts will be listed")

    dagobah = Dagobah(backend, event_handler, ssh_config)
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

    email_handler = get_email_handler(get_conf(config, 'Dagobahd.email', None),
                                      get_conf(config, 'Email', {}))

    if (email_handler and
        get_conf(config, 'Email.send_on_success', False) == True):
        handler.register('job_complete', job_complete_email, email_handler)

    if (email_handler and
        get_conf(config, 'Email.send_on_failure', False) == True):
        handler.register('job_failed', job_failed_email, email_handler)
        handler.register('task_failed', task_failed_email, email_handler)

    return handler


def init_logger(location, config):
    """ Initialize the logger with settings from config. """

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    if get_conf(config, 'Logging.enabled', False) == False:
        handler = NullHandler()
        logging.getLogger("dagobah").addHandler(handler)
        return

    if get_conf(config, 'Logging.logfile', 'default') == 'default':
        path = os.path.join(location, 'dagobah.log')
    else:
        path = config['Logging']['logfile']

    level_string = get_conf(config, 'Logging.loglevel', 'info').upper()
    numeric_level = getattr(logging, level_string, None)

    logging.basicConfig(filename=path, level=numeric_level)

    root = logging.getLogger()
    stdout_logger = logging.StreamHandler(sys.stdout)
    stdout_logger.setLevel(logging.INFO)
    root.addHandler(stdout_logger)

    print 'Logging output to %s' % path
    logging.info('Logger initialized at level %s' % level_string)


def get_backend(config):
    """ Returns a backend instance based on the Daemon config file. """

    backend_string = get_conf(config, 'Dagobahd.backend', None)

    if backend_string is None:
        from ..backend.base import BaseBackend
        return BaseBackend()

    elif backend_string.lower() == 'sqlite':
        backend_kwargs = {}
        for conf_kwarg in ['filepath']:
            backend_kwargs[conf_kwarg] = get_conf(config,
                                                  'SQLiteBackend.%s' % conf_kwarg)

        try:
            from ..backend.sqlite import SQLiteBackend
        except:
            raise ImportError('Could not initialize the SQLite Backend. Are you sure' +
                              ' the optional drivers are installed? If not, try running ' +
                              '"pip install pysqlite sqlalchemy alembic" to install them.')
        return SQLiteBackend(**backend_kwargs)

    elif backend_string.lower() == 'mongo':
        backend_kwargs = {}
        for conf_kwarg in ['host', 'port', 'db',
                           'dagobah_collection', 'job_collection',
                           'log_collection']:
            backend_kwargs[conf_kwarg] = get_conf(config,
                                                  'MongoBackend.%s' % conf_kwarg)
        backend_kwargs['port'] = int(backend_kwargs['port'])

        try:
            from ..backend.mongo import MongoBackend
        except:
            raise ImportError('Could not initialize the MongoDB Backend. Are you sure' +
                              ' the optional drivers are installed? If not, try running ' +
                              '"pip install pymongo" to install them.')
        return MongoBackend(**backend_kwargs)

    raise ValueError('unknown backend type specified in conf')


@app.route('/favicon.ico')
def favicon_redirect():
    return send_from_directory(os.path.join(app.root_path,
                                            'static', 'img'),
                               'favicon.ico',
                               mimetype='image/vnd.microsoft.icon')


config = get_config_file()
dagobah = init_dagobah()
app.config['dagobah'] = dagobah
configure_app()
