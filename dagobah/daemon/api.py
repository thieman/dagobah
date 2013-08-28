""" HTTP API methods for Dagobah daemon. """

from flask import request, abort
from flask_login import login_required

from dagobah.daemon.daemon import app
from dagobah.daemon.util import validate_dict, api_call

dagobah = app.config['dagobah']

@app.route('/api/jobs', methods=['GET'])
@login_required
@api_call
def get_jobs():
    return dagobah._serialize().get('jobs', {})


@app.route('/api/job', methods=['GET'])
@login_required
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
@login_required
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
@login_required
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
@login_required
@api_call
def add_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.add_job(args['job_name'])


@app.route('/api/delete_job', methods=['POST'])
@login_required
@api_call
def delete_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name'],
                         job_name=str):
        abort(400)

    dagobah.delete_job(args['job_name'])


@app.route('/api/start_job', methods=['POST'])
@login_required
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
@login_required
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
@login_required
@api_call
def add_task_to_job():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_command', 'task_name'],
                         job_name=str,
                         task_command=str,
                         task_name=str):
        abort(400)

    if args.get('task_target', None):
        dagobah.add_task_to_job(args['job_name'],
                                args['task_command'],
                                args['task_name'],
                                task_target=args['task_target'][0],
                                task_target_key=args['task_target_key'][0],
                                task_target_password=args['task_target_password'][0])
    else:
        dagobah.add_task_to_job(args['job_name'],
                                args['task_command'],
                                args['task_name'])


@app.route('/api/delete_task', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
@api_call
def stop_scheduler():
    dagobah.scheduler.stop()


@app.route('/api/restart_scheduler', methods=['POST'])
@login_required
@api_call
def restart_scheduler():
    dagobah.scheduler.restart()


@app.route('/api/terminate_all_tasks', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
@api_call
def edit_task():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_name'],
                         job_name=str,
                         task_name=str,
                         name=str,
                         command=str,
                         soft_timeout=int,
                         hard_timeout=int):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)

    del args['job_name']
    del args['task_name']
    job.edit_task(task.name, **args)


@app.route('/api/set_soft_timeout', methods=['POST'])
@login_required
@api_call
def set_soft_timeout():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_name', 'soft_timeout'],
                         job_name=str,
                         task_name=str,
                         soft_timeout=int):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)

    task.set_soft_timeout(args['soft_timeout'])


@app.route('/api/set_hard_timeout', methods=['POST'])
@login_required
@api_call
def set_hard_timeout():
    args = dict(request.form)
    if not validate_dict(args,
                         required=['job_name', 'task_name', 'hard_timeout'],
                         job_name=str,
                         task_name=str,
                         hard_timeout=int):
        abort(400)

    job = dagobah.get_job(args['job_name'])
    task = job.tasks.get(args['task_name'], None)
    if not task:
        abort(400)

    task.set_hard_timeout(args['hard_timeout'])
