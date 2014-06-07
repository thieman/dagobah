""" Views for Dagobah daemon. """

from flask import render_template, redirect, url_for, abort
from flask_login import login_required

from .daemon import app
from .api import get_jobs, import_job

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
    jobs = [job for job in get_jobs() if str(job['job_id']) == job_id]
    if not jobs:
        abort(404)
    return render_template('job_detail.html', job=jobs[0], hosts=dagobah.get_hosts())

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

@app.route('/job/<job_id>/<task_name>/<log_id>', methods=['GET'])
@login_required
def log_detail(job_id=None, task_name=None, log_id=None):
        """ Show a detailed description of a specific log. """
        jobs = get_jobs()
        job = [job for job in jobs if str(job['job_id']) == job_id][0]
        return render_template('log_detail.html',
                               job=job,
                               task_name=task_name,
                               task=[task for task in job['tasks']
                                     if task['name'] == task_name][0],
                               log_id=log_id)

@app.route('/settings', methods=['GET'])
@login_required
def settings_view():
    """ View for managing app-wide configuration. """
    return render_template('settings.html')
