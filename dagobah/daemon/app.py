from .daemon import app, login_manager
from .auth import *
from .api import *
from .views import *

def daemon_entrypoint():
    # TODO: the Flask reloader causes multiple Dagobah instances to get created
    # with two schedulers. Need a fix to reenable Flask reloading.
    print 'Starting app on %s:%s' % (app.config['APP_HOST'],
                                     app.config['APP_PORT'])
    app.run(host=app.config['APP_HOST'], port=app.config['APP_PORT'],
            use_reloader=False)

if __name__ == '__main__':
    daemon_entrypoint()
