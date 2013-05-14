""" HTTP Daemon implementation for Dagobah service. """

import os

from flask import Flask, send_from_directory

from dagobah.core import Dagobah
from dagobah.backend.mongo import MongoBackend

app = Flask(__name__)

APP_PORT = 9000


def init_dagobah():
    backend = MongoBackend(host='localhost', port=27017, db='dagobah')
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
