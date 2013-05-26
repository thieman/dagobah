from dagobah.daemon.daemon import app, APP_PORT
from dagobah.daemon.api import *
from dagobah.daemon.views import *

def daemon_entrypoint():
    app.debug = False

    # TODO: the Flask reloader causes multiple Dagobah instances to get created
    # with two schedulers. Need a fix to reenable Flask reloading.
    app.run(host='0.0.0.0', port=APP_PORT, use_reloader=False)

if __name__ == '__main__':
    daemon_entrypoint()
