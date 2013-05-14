""" Views for Dagobah daemon. """

from flask import render_template, redirect, url_for

from dagobah.daemon.daemon import app
from dagobah.daemon.api import get_jobs

dagobah = app.config['dagobah']


@app.route('/', methods=['GET'])
def index_route():
    """ Redirect to the dashboard. """
    return redirect(url_for('dashboard'))


@app.route('/dashboard', methods=['GET'])
def dashboard():
    """ Eventually might have some stuff. """
    return render_template('dashboard.html')


@app.route('/jobs', methods=['GET'])
def jobs():
    """ Show information on all known Jobs. """
    return render_template('jobs.html',
                           jobs=get_jobs())
