""" Tests on the core class implementations (Dagobah, Job, Task) """

from datetime import datetime
from time import sleep

from nose import with_setup
from nose.tools import nottest, raises

from dagobah.core.core import Dagobah, Job, Task
from dagobah.backend.base import BaseBackend

dagobah = None

@nottest
def blank_dagobah():
    global dagobah
    backend = BaseBackend()
    dagobah = Dagobah(backend)


@with_setup(blank_dagobah)
def test_dagobah_add_job():
    dagobah.add_job('test_job')


@with_setup(blank_dagobah)
@raises(KeyError)
def test_dagobah_add_job_unavailable_name():
    dagobah.add_job('test_job')
    dagobah.add_job('test_job')


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
@raises(KeyError)
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
@raises(ValueError)
def test_job_states_invalid():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')
    job._set_status('bogus')


@with_setup(blank_dagobah)
def test_job_schedule():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')

    job.base_datetime = datetime(2012, 1, 1, 1, 0, 0)
    job.schedule('5 * * * *')
    assert job.next_run == datetime(2012, 1, 1, 1, 5, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 1, 2, 5, 0)

    job.schedule('*/5 * * * *')
    assert job.next_run == datetime(2012, 1, 1, 1, 5, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 1, 1, 10, 0)

    job.schedule('0 5 * * *')
    assert job.next_run == datetime(2012, 1, 1, 5, 0, 0)
    job.next_run = job.cron_iter.get_next(datetime)
    assert job.next_run == datetime(2012, 1, 2, 5, 0, 0)

    job.schedule('0 0 * * 3')
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
@raises(KeyError)
def test_add_task_to_job_bad_job():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job_2', 'ls')


@with_setup(blank_dagobah)
def test_run_job():
    dagobah.add_job('test_job')
    dagobah.add_task_to_job('test_job', 'ls', 'list')
    job = dagobah.get_job('test_job')
    job.start()

    tries = 0
    while tries < 5:
        if job.status != 'running':
            assert job.status != 'failed'
            return
        sleep(1)
        tries += 1

    raise ValueError('test timed out')
