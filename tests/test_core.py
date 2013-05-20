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
def test_dagobah_delete_job():
    dagobah.add_job('test_job')
    assert len(dagobah.jobs) == 1
    dagobah.delete_job('test_job')
    assert len(dagobah.jobs) == 0


@with_setup(blank_dagobah)
@raises(KeyError)
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


@with_setup(blank_dagobah)
@raises(ValueError)
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
                                        'success': None},
                                       {'command': 'grep',
                                        'name': 'grep',
                                        'completed_at': None,
                                        'started_at': None,
                                        'success': None},],
                             'dependencies': {'list': ['grep'],
                                              'grep': []},
                             'status': 'waiting',
                             'cron_schedule': '*/5 * * * *',
                             'next_run': datetime(2012, 1, 1, 1, 5, 0)}]}
    print dagobah._serialize()
    print test_result
    assert dagobah._serialize() == test_result


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
        if job.status == 'running':
            for task in job.tasks.values():
                task.terminate()
            return
        sleep(1)

    raise ValueError('scheduler did not start job')
