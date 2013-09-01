""" Views for Dagobah daemon. """

from flask import render_template, redirect, url_for
from flask_login import login_required

from dagobah.daemon.daemon import app
from dagobah.daemon.api import get_jobs, import_job

dagobah = app.config['dagobah']


@app.route('/', methods=['GET'])
def index_route():
    """ Redirect to the dashboard. """
    return redirect(url_for('jobs'))

@app.route('/jobs', methods=['GET'])
@login_required
def jobs():
    """ Show information on all known Jobs. """
    return render_template('jobs.html',
                           jobs=get_jobs())

@app.route('/jobs/import', methods=['POST'])
@login_required
def jobs_import_view():
    """ Import a Job and redirect to the Jobs page. """
    import_job()
    return redirect(url_for('jobs'))


@app.route('/job/<job_id>', methods=['GET'])
@login_required
def job_detail(job_id=None):
    """ Show a detailed description of a Job's status. """
    jobs = get_jobs()
    job = [job for job in jobs if str(job['job_id']) == job_id][0]
    return render_template('job_detail.html', job=job)

@app.route('/job/<job_id>/<task_name>', methods=['GET'])
@login_required
def task_detail(job_id=None, task_name=None):
    """ Show a detailed description of a specific task. """
    jobs = get_jobs()
    job = [job for job in jobs if str(job['job_id']) == job_id][0]
    return render_template('task_detail.html',
                           job=job,
                           task_name=task_name,
                           task=[task for task in job['tasks']
                                 if task['name'] == task_name][0])


@app.route('/settings', methods=['GET'])
@login_required
def settings_view():
    """ View for managing app-wide configuration. """
    return render_template('settings.html')
