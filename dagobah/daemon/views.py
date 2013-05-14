""" Views for Dagobah daemon. """

from flask import render_template, redirect, url_for

from dagobah.daemon.daemon import app

dagobah = app.config['dagobah']


@app.route('/', methods=['GET'])
def index_route():
    return redirect(url_for('dashboard'))


@app.route('/dashboard', methods=['GET'])
def dashboard():
    return render_template('dashboard.html')


@app.route('/jobs', methods=['GET'])
def jobs():
    return render_template('jobs.html')
