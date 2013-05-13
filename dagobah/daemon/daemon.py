""" HTTP Daemon implementation for Dagobah service. """

from flask import Flask, render_template, abort

from dagobah.core import Dagobah
from dagobah.backend.mongo import MongoBackend
from dagobah.daemon.util import validate_dict, api_call

app = Flask(__name__)

APP_PORT = 9000
DAGOBAH_BACKEND = MongoBackend(host='localhost', port='27018')

dagobah = Dagobah(DAGOBAH_BACKEND)
app.config['dagobah'] = dagobah


@app.route('/', methods=['GET'])
def index_route():
    return render_template('index.html')


@app.route('/api/jobs', methods=['GET'])
@api_call
def get_jobs():
    return dagobah._serialize().get('jobs', {})


@app.route('/api/head', methods=['GET'])
@api_call
def head_task():

    args = dict(request.args)
    if not validate_dict(request.args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str,
                         stream=str,
                         num_lines=int):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)
    return task.head(**args)


@app.route('/api/tail', methods=['GET'])
@api_call
def tail_task():

    args = dict(request.args)
    if not validate_dict(request.args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str,
                         stream=str,
                         num_lines=int):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)
    return task.tail(**args)


@app.route('/api/add_job', methods=['POST'])
@api_call
def add_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.add_job(args['job_name'])


@app.route('/api/delete_job', methods=['POST'])
@api_call
def delete_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.delete_job(args['job_name'])


@app.route('/api/start_job', methods=['POST'])
@api_call
def start_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.start()


@app.route('/api/retry_job', methods=['POST'])
@api_call
def retry_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.retry()


@app.route('/api/add_task_to_job', methods=['POST'])
@api_call
def add_task_to_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name', 'task_command', 'task_name'],
                         job_name=str,
                         task_command=str,
                         task_name=str):
        abort(400)

    dagobah.add_task_to_job(args['job_name'],
                            args['task_command'],
                            args['task_name'])


@app.route('/api/add_dependency', methods=['POST'])
@api_call
def add_dependency():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name',
                                   'from_task_name',
                                   'to_task_name'],
                         job_name=str,
                         from_task_name=str,
                         to_task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.add_edge(args['from_task_name'], args['to_task_name'])


@app.route('/api/schedule_job', methods=['POST'])
@api_call
def schedule_job():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name', 'cron_schedule'],
                         job_name=str,
                         cron_schedule=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.schedule(args['cron_schedule'])


@app.route('/api/stop_scheduler', methods=['POST'])
@api_call
def stop_scheduler():
    dagobah.scheduler.stop()


@app.route('/api/restart_scheduler', methods=['POST'])
@api_call
def restart_scheduler():
    dagobah.scheduler.restart()


@app.route('/api/terminate_task', methods=['POST'])
@api_call
def terminate_task():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)
    task.terminate()


@app.route('/api/kill_task', methods=['POST'])
@api_call
def kill_task():

    args = dict(request.form)
    if not validate_dict(request.form,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)
    task.kill()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=APP_PORT)
