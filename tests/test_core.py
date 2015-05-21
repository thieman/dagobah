""" Tests on the core class implementations (Dagobah, Job, Task) """

from datetime import datetime
from time import sleep
import signal
from functools import wraps

from nose import with_setup
from nose.tools import nottest, raises, assert_equal

from dagobah.core.core import Dagobah, Job, Task, DagobahError
from dagobah.backend.base import BaseBackend

import os

dagobah = None

class DagobahTestTimeoutException(Exception):
    pass

@nottest
def raise_timeout_exception(*args, **kwargs):
    raise DagobahTestTimeoutException()

@nottest
def wait_until_stopped(job):
    while job.state.status == 'running':
        sleep(0.1)
        continue

@nottest
def supports_timeouts(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        signal.signal(signal.SIGALRM, raise_timeout_exception)
        result = fn(*args, **kwargs)
        signal.alarm(0)
        signal.signal(signal.SIGALRM, lambda signalnum, handler: None)
        return result
    return wrapped

@nottest
def blank_dagobah():
    global dagobah
    backend = BaseBackend()
    location = os.path.realpath(os.path.join(os.getcwd(),
                                             os.path.dirname(__file__)))
    dagobah = Dagobah(backend, ssh_config=os.path.join(location,
                                                       "test_ssh_config"))


@with_setup(blank_dagobah)
def test_dagobah_add_job():
    dagobah.add_job('test_job')


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_dagobah_add_job_unavailable_name():
    dagobah.add_job('test_job')
    dagobah.add_job('test_job')


@with_setup(blank_dagobah)
def test_dagobah_delete_job():
    dagobah.add_job('test_job')
    assert len(dagobah.jobs) == 1
    dagobah.delete_job('test_job')
    assert len(dagobah.jobs) == 0


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_dagobah_delete_job_does_not_exist():
    dagobah.delete_job('test_job')


@with_setup(blank_dagobah)
def test_dagobah_get_job_does_not_exist():
    assert dagobah.get_job('test_job') is None


@with_setup(blank_dagobah)
def test_dagobah_get_job():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    assert job.name == 'test_job'


@with_setup(blank_dagobah)
def test_dagobah_add_tasks():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    job.add_task('python /etc/do_stuff.py')
    job.add_task('python /var/log/do_other_stuff.py', 'job two')
    assert set(job.tasks.keys()) == set(['python /etc/do_stuff.py',
                                         'job two'])


@with_setup(blank_dagobah)
def test_dagobah_delete_job():
    dagobah.add_job('test_job')
    dagobah.delete_job('test_job')


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_dagobah_delete_job_does_not_exist():
    dagobah.add_job('test_job')
    dagobah.delete_job('test_job_2')


@with_setup(blank_dagobah)
def test_job_states():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    for valid_status in ['waiting', 'running', 'failed']:
        job._set_status(valid_status)


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_job_states_invalid():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    job._set_status('bogus')


@with_setup(blank_dagobah)
def test_job_schedule():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')

    base_datetime = datetime(2012, 1, 1, 1, 0, 0)
    job.schedule('5 * * * *', base_datetime)
    assert job.next_run == datetime(2012, 1, 1, 1, 5, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 1, 2, 5, 0)

    job.schedule('*/5 * * * *', base_datetime)
    assert job.next_run == datetime(2012, 1, 1, 1, 5, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 1, 1, 10, 0)

    job.schedule('0 5 * * *', base_datetime)
    assert job.next_run == datetime(2012, 1, 1, 5, 0, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 2, 5, 0, 0)

    job.schedule('0 0 * * 3', base_datetime)
    assert job.next_run == datetime(2012, 1, 4, 0, 0, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 11, 0, 0, 0)


@with_setup(blank_dagobah)
def test_add_task_to_job():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    dagobah.add_task_to_job('test_job', 'ls', 'list')
    dagobah.add_task_to_job(job, 'cat', 'concat')
    dagobah.add_task_to_job('test_job', 'wc')
    dagobah.add_task_to_job(job, 'sed')
    assert set(job.tasks.keys()) == set(['list', 'concat', 'wc', 'sed'])


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_add_task_to_job_bad_job():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job_2', 'ls')


@with_setup(blank_dagobah)
@supports_timeouts
def test_run_job():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job', 'ls', 'list')
    job = dagobah.get_job('test_job')

    signal.alarm(10)
    job.start()

    wait_until_stopped(job)
    assert job.state.status != 'failed'


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_start_running_job():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job', 'ls', 'list')
    job = dagobah.get_job('test_job')
    job.start()
    job.start()


@with_setup(blank_dagobah)
def test_serialize_dagobah():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    job.add_task('ls', 'list')
    job.add_task('grep', 'grep')
    job.add_edge('list', 'grep')
    base_datetime = datetime(2012, 1, 1, 1, 0, 0)
    job.schedule('*/5 * * * *', base_datetime)
    job.update_job_notes("Here are some notes")
    dagobah_id = dagobah.dagobah_id
    test_result = {'dagobah_id': dagobah_id,
                   'created_jobs': 1,
                   'jobs': [{'job_id': job.job_id,
                             'name': 'test_job',
                             'parent_id': dagobah_id,
                             'tasks': [{'command': 'ls',
                                        'name': 'list',
                                        'completed_at': None,
                                        'started_at': None,
                                        'success': None,
                                        'soft_timeout': 0,
                                        'hard_timeout': 0,
                                        'hostname': None},
                                       {'command': 'grep',
                                        'name': 'grep',
                                        'completed_at': None,
                                        'started_at': None,
                                        'success': None,
                                        'soft_timeout': 0,
                                        'hard_timeout': 0,
                                        'hostname': None},],
                             'dependencies': {'list': ['grep'],
                                              'grep': []},
                             'status': 'waiting',
                             'cron_schedule': '*/5 * * * *',
                             'next_run': datetime(2012, 1, 1, 1, 5, 0),
                             'notes': 'Here are some notes'}]}
    print dagobah._serialize()
    print test_result
    assert_equal(dagobah._serialize(), test_result)


@with_setup(blank_dagobah)
def test_scheduler_monitoring():
    return  # reenable me at some point, please, I just take too long for dev
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    job.add_task('sleep 60')
    curr_minute = datetime.now().minute
    if datetime.now().second >= 58:
        curr_minute += 1
    job.schedule('%d * * * *' % ((curr_minute + 1) % 60))

    for i in range(65):
        if job.state.status == 'running':
            for task in job.tasks.values():
                task.terminate()
            return
        sleep(1)

    raise ValueError('scheduler did not start job')


@with_setup(blank_dagobah)
def test_construct_with_timeouts():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job', 'echo "dagobah"', 'From Dagobah',
                            soft_timeout=60, hard_timeout=120)
    job = dagobah.get_job('test_job')
    job.add_task('echo "job"', 'From Job', soft_timeout=10, hard_timeout=20)
    assert job.tasks['From Dagobah'].soft_timeout == 60
    assert job.tasks['From Dagobah'].hard_timeout == 120
    assert job.tasks['From Job'].soft_timeout == 10
    assert job.tasks['From Job'].hard_timeout == 20

@with_setup(blank_dagobah)
def test_ssh_config_load():
    hosts = dagobah.get_hosts()
    assert "test_host" in hosts
    assert "*" not in hosts
    assert "nonexistant" not in hosts

@with_setup(blank_dagobah)
@supports_timeouts
def test_retry_from_failure():
    """
    Test retry after job failure

    This sets up a job with 3 tasks, the 2nd of which will fail. Upon
    successful failure, the failed task is corrected, and then run again,
    whereupon it should succeed.
    """
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job', 'pwd', 'a')
    dagobah.add_task_to_job('test_job', 'false', 'b')
    dagobah.add_task_to_job('test_job', 'ls', 'c')
    job = dagobah.get_job('test_job')

    job.add_dependency('a', 'b')
    job.add_dependency('b', 'c')

    signal.alarm(10)
    job.start()

    wait_until_stopped(job)
    assert job.state.status == 'failed'

    signal.alarm(10)
    job.edit_task("b", command="true")
    job.retry()

    wait_until_stopped(job)
    assert job.state.status != 'failed'
