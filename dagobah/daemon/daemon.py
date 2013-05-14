""" HTTP Daemon implementation for Dagobah service. """

import os

from flask import Flask, send_from_directory

from dagobah.core import Dagobah
from dagobah.backend.mongo import MongoBackend

app = Flask(__name__)

APP_PORT = 9000
DAGOBAH_BACKEND = MongoBackend(host='localhost', port=27018,
                               db='dagobah')

dagobah = Dagobah(DAGOBAH_BACKEND)
app.config['dagobah'] = dagobah


@app.route('/favicon.ico')
def favicon_redirect():
    return send_from_directory(os.path.join(app.root_path,
                                            'static', 'img'),
                               'favicon.ico',
                               mimetype='image/vnd.microsoft.icon')


from dagobah.daemon.api import *
from dagobah.daemon.views import *


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=APP_PORT)
