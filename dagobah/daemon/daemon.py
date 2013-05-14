""" HTTP Daemon implementation for Dagobah service. """

from flask import Flask

from dagobah.core import Dagobah
from dagobah.backend.mongo import MongoBackend

app = Flask(__name__)

APP_PORT = 9000
DAGOBAH_BACKEND = MongoBackend(host='localhost', port=27018,
                               db='dagobah')

dagobah = Dagobah(DAGOBAH_BACKEND)
app.config['dagobah'] = dagobah


from dagobah.daemon.api import *
from dagobah.daemon.views import *


if __name__ == '__main__':
    app.debug = False
    app.run(host='0.0.0.0', port=APP_PORT)
