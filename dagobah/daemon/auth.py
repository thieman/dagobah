""" Authentication classes and views for Dagobahd. """

from datetime import datetime, timedelta

from flask import render_template, request, url_for, redirect
from flask_login import UserMixin, login_user, logout_user, login_required

from .app import app, login_manager

class User(UserMixin):
    def get_id(self):
        return 1

SingleAuthUser = User()


@login_manager.user_loader
def load_user(userid):
    return SingleAuthUser


@app.route('/login', methods=['GET'])
def login():
    return render_template('login.html', alert=request.args.get('alert'))


@app.route('/do-login', methods=['POST'])
def do_login():
    """ Attempt to auth using single login. Rate limited at the site level. """

    dt_filter = lambda x: x >= datetime.utcnow() - timedelta(seconds=60)
    app.config['AUTH_ATTEMPTS'] = filter(dt_filter, app.config['AUTH_ATTEMPTS'])

    if len(app.config['AUTH_ATTEMPTS']) > app.config['AUTH_RATE_LIMIT']:
        return redirect(url_for('login',
                                alert="Rate limit exceeded. Try again in 60 seconds."))

    if request.form.get('password') == app.config['APP_PASSWORD']:
        login_user(SingleAuthUser)
        return redirect('/')

    app.config['AUTH_ATTEMPTS'].append(datetime.utcnow())
    return redirect(url_for('login', alert="Incorrect password."))


@app.route('/do-logout', methods=['GET', 'POST'])
@login_required
def do_logout():
    logout_user()
    return redirect(url_for('login'))
