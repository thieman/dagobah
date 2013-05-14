""" Views for Dagobah daemon. """

from flask import render_template

from dagobah.daemon.daemon import app

dagobah = app.config['dagobah']


@app.route('/', methods=['GET'])
def index_route():
    return render_template('index.html')
