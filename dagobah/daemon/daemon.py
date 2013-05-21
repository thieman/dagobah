""" HTTP Daemon implementation for Dagobah service. """

import os
from ConfigParser import ConfigParser

from flask import Flask, send_from_directory

from dagobah.core import Dagobah
from dagobah.backend.base import BaseBackend
from dagobah.backend.mongo import MongoBackend

app = Flask(__name__)
APP_PORT = 9000


def init_dagobah(testing=False):

    location = os.path.realpath(os.path.join(os.getcwd(),
                                             os.path.dirname(__file__)))

    config = ConfigParser()
    config.read(os.path.join(location, 'dagobahd.conf'))

    backend = get_backend(config)
    dagobah = Dagobah(backend)

    known_ids = [id for id in backend.get_known_dagobah_ids()
                 if id != dagobah.dagobah_id]
    if len(known_ids) > 1:
        # need a way to handle this intelligently through config
        raise ValueError('could not infer dagobah ID, ' +
                         'multiple available in backend')

    if known_ids:
        dagobah.from_backend(known_ids[0])

    return dagobah


def get_backend(config):
    """ Returns a backend instance based on the Daemon config file. """

    backend_string = config.get('Dagobahd', 'backend')

    if backend_string.lower() == 'none':
        return BaseBackend()

    elif backend_string.lower() == 'mongo':

        backend_kwargs = {}
        for conf_kwarg in ['host', 'port', 'db',
                           'dagobah_collection', 'job_collection',
                           'log_collection']:
            backend_kwargs[conf_kwarg] = config.get('MongoBackend', conf_kwarg)

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
