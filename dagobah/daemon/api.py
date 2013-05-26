""" HTTP API methods for Dagobah daemon. """

from flask import request, abort

from dagobah.daemon.daemon import app
from dagobah.daemon.util import validate_dict, api_call

dagobah = app.config['dagobah']

@app.route('/api/jobs', methods=['GET'])
@api_call
def get_jobs():
    return dagobah._serialize().get('jobs', {})


@app.route('/api/job', methods=['GET'])
@api_call
def get_job():
    args = dict(request.args)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    return job._serialize()


@app.route('/api/head', methods=['GET'])
@api_call
def head_task():
    args = dict(request.args)
    if not validate_dict(args,
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

    call_args = {}
    for key in ['stream', 'num_lines']:
        if key in args:
            call_args[key] = args[key]
    return task.head(**call_args)


@app.route('/api/tail', methods=['GET'])
@api_call
def tail_task():
    args = dict(request.args)
    if not validate_dict(args,
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

    call_args = {}
    for key in ['stream', 'num_lines']:
        if key in args:
            call_args[key] = args[key]
    return task.tail(**call_args)


@app.route('/api/add_job', methods=['POST'])
@api_call
def add_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.add_job(args['job_name'])


@app.route('/api/delete_job', methods=['POST'])
@api_call
def delete_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.delete_job(args['job_name'])


@app.route('/api/start_job', methods=['POST'])
@api_call
def start_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.start()


@app.route('/api/retry_job', methods=['POST'])
@api_call
def retry_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.retry()


@app.route('/api/add_task_to_job', methods=['POST'])
@api_call
def add_task_to_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_command', 'task_name'],
                         job_name=str,
                         task_command=str,
                         task_name=str):
        abort(400)

    dagobah.add_task_to_job(args['job_name'],
                            args['task_command'],
                            args['task_name'])


@app.route('/api/delete_task', methods=['POST'])
@api_call
def delete_task():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.delete_task(args['task_name'])


@app.route('/api/add_dependency', methods=['POST'])
@api_call
def add_dependency():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name',
                                   'from_task_name',
                                   'to_task_name'],
                         job_name=str,
                         from_task_name=str,
                         to_task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.add_dependency(args['from_task_name'], args['to_task_name'])


@app.route('/api/delete_dependency', methods=['POST'])
@api_call
def delete_dependency():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name',
                                   'from_task_name',
                                   'to_task_name'],
                         job_name=str,
                         from_task_name=str,
                         to_task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.delete_dependency(args['from_task_name'], args['to_task_name'])


@app.route('/api/schedule_job', methods=['POST'])
@api_call
def schedule_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'cron_schedule'],
                         job_name=str,
                         cron_schedule=str):
        abort(400)

    if args['cron_schedule'] == '':
        args['cron_schedule'] = None

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


@app.route('/api/terminate_all_tasks', methods=['POST'])
@api_call
def terminate_all_tasks():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.terminate_all()


@app.route('/api/kill_all_tasks', methods=['POST'])
@api_call
def kill_all_tasks():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    job.kill_all()


@app.route('/api/terminate_task', methods=['POST'])
@api_call
def terminate_task():
    args = dict(request.form)
    if not validate_dict(args,
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
    if not validate_dict(args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)
    task.kill()


@app.route('/api/edit_job', methods=['POST'])
@api_call
def edit_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str,
                         name=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    del args['job_name']
    job.edit(**args)


@app.route('/api/edit_task', methods=['POST'])
@api_call
def edit_task():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str,
                         name=str,
                         command=str):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)

    del args['job_name']
    del args['task_name']
    job.edit_task(task.name, **args)
