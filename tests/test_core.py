""" Tests on the core class implementations (Dagobah, Job, Task) """

from datetime import datetime
from time import sleep
import signal
from functools import wraps

from nose import with_setup
from nose.tools import nottest, raises, assert_equal

from dagobah.core.dagobah import Dagobah
from dagobah.core.dagobah_error import DagobahError
from dagobah.backend.base import BaseBackend

from pprint import pprint, pformat

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
def assert_graph_equality(a, b):
    """ This is a utility to do dictionary comparisons for DAG graphs """
    # Empty Graph
    if len(a) == 0 and len(b) == 0:
        return True

    # Verify the keys are the same
    diff = set(b.keys()) - set(a.keys())
    if len(diff) != 0:
        return False

    # Start to verify contents
    for key, a_val in a.iteritems():
        b_val = b[key]
        edge_diff = a_val - b_val
        if len(edge_diff) != 0:
            return False
    return True


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
def test_dagobah_delete_job2():
    dagobah.add_job('test_job')
    dagobah.delete_job('test_job')


@with_setup(blank_dagobah)
@raises(DagobahError)
def test_dagobah_delete_job_does_not_exist2():
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
                                        'hostname': None}, ],
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


@with_setup(blank_dagobah)
def test_dagobah_add_jobtasks():
    dagobah.add_job('test_job_a')
    dagobah.add_job('test_job_b')
    job_a = dagobah.get_job('test_job_a')
    job_b = dagobah.get_job('test_job_b')
    job_a.add_task('ls')
    job_b.add_jobtask('test_job_a')
    assert "test_job_a" in [t.target_job_name for t in job_b.tasks.values()]


@with_setup(blank_dagobah)
def test_verify_no_jobtasks():
    dagobah.add_job('test_job')
    job = dagobah.get_job('test_job')

    job.add_task('ls')
    job.add_task('pwd')
    job.add_task('sleep 1')

    job.add_edge('ls', 'pwd')
    job.add_edge('pwd', 'sleep 1')
    assert job.verify()


@with_setup(blank_dagobah)
def test_verify_with_jobtasks():
    dagobah.add_job('A')
    dagobah.add_job('B')
    a = dagobah.get_job('A')
    b = dagobah.get_job('B')

    a.add_task('ls')
    b.add_task('pwd')
    b.add_jobtask('A')
    b.add_edge('pwd', 'A')

    assert a.verify()
    assert b.verify()


@with_setup(blank_dagobah)
def test_verify_detect_cycle():
    dagobah.add_job('A')
    dagobah.add_job('B')
    a = dagobah.get_job('A')
    b = dagobah.get_job('B')

    a.add_jobtask('A')
    b.add_jobtask('B')

    assert not a.verify()
    assert not b.verify()


@with_setup(blank_dagobah)
def test_verify_detect_cycle_complex():
    dagobah.add_job('A')
    dagobah.add_job('B')
    dagobah.add_job('C')
    dagobah.add_job('D')
    dagobah.add_job('E')

    a = dagobah.get_job('A')
    b = dagobah.get_job('B')
    c = dagobah.get_job('C')
    d = dagobah.get_job('D')
    e = dagobah.get_job('E')

    a.add_jobtask('B')
    a.add_task('pwd')
    a.add_task('ls')
    a.add_edge('pwd', 'B')
    a.add_edge('ls', 'B')
    assert a.verify()

    b.add_jobtask('C')
    b.add_jobtask('D')
    b.add_task('yes')
    b.add_task('pwd')
    b.add_task('ls -lahtr')
    b.add_task('date')
    b.add_edge('yes', 'ls -lahtr')
    b.add_edge('pwd', 'ls -lahtr')
    b.add_edge('ls -lahtr', 'C')
    b.add_edge('ls -lahtr', 'D')
    b.add_edge('D', 'date')
    b.add_edge('C', 'date')
    assert b.verify()

    c.add_task('ls /home')
    c.add_jobtask('D')
    c.add_task('cd .')
    c.add_edge('ls /home', 'D')
    c.add_edge('D', 'cd .')
    assert c.verify()

    d.add_jobtask('E')
    d.add_task('ls')
    d.add_task('pwd')
    d.add_task('cat')
    d.add_edge('ls', 'E')
    d.add_edge('pwd', 'E')
    d.add_edge('pwd', 'cat')
    d.add_edge('E', 'cat')
    assert d.verify()

    e.add_task('ls')
    e.add_task('pwd')
    e.add_jobtask('B')

    assert not a.verify()
    assert not b.verify()
    assert not c.verify()
    assert not d.verify()
    assert not e.verify()


@with_setup(blank_dagobah)
def test_dagobah_expand_none():
    dagobah.add_job('test_job_a')
    job_a = dagobah.get_job('test_job_a')
    job_a.add_task('ls')
    graph, tasks = job_a.expand(job_a.graph, job_a.tasks)
    ideal_result = {'ls': set([])}
    assert assert_graph_equality(ideal_result, graph)


@with_setup(blank_dagobah)
def test_dagobah_expand_empty_job():
    dagobah.add_job('test_job_a')
    dagobah.add_job('test_job_b')
    job_b = dagobah.get_job('test_job_b')
    job_b.add_task('ls')
    job_b.add_task('pwd')
    job_b.add_jobtask('test_job_a')
    job_b.add_edge('ls', 'test_job_a')
    job_b.add_edge('test_job_a', 'pwd')
    graph, tasks = job_b.expand(job_b.graph, job_b.tasks)
    pprint(graph)
    pprint(tasks)
    ideal_result = {'ls': set(['pwd']), 'pwd': set([])}
    assert assert_graph_equality(ideal_result, graph)


@with_setup(blank_dagobah)
def test_dagobah_expand_simple_job():
    dagobah.add_job('test_job_a')
    dagobah.add_job('test_job_b')
    job_a = dagobah.get_job('test_job_a')
    job_a.add_task('yes')
    job_b = dagobah.get_job('test_job_b')
    job_b.add_task('ls')
    job_b.add_task('pwd')
    job_b.add_jobtask('test_job_a')
    job_b.add_edge('ls', 'test_job_a')
    job_b.add_edge('test_job_a', 'pwd')
    graph, tasks = job_b.expand(job_b.graph, job_b.tasks)
    ideal_result = {
        'ls': set(['test_job_a_yes']),
        'pwd': set([]),
        'test_job_a_yes': set(['pwd'])
    }
    assert assert_graph_equality(ideal_result, graph)


@with_setup(blank_dagobah)
def test_dagobah_expand_moderate_job():
    dagobah.add_job('test_job_a')
    dagobah.add_job('test_job_b')
    job_a = dagobah.get_job('test_job_a')
    job_a.add_task('yes')
    job_a.add_task('ls')
    job_a.add_task('cd .')
    job_a.add_edge('yes', 'ls')
    job_a.add_edge('ls', 'cd .')

    job_b = dagobah.get_job('test_job_b')
    job_b.add_task('ls')
    job_b.add_task('pwd')
    job_b.add_jobtask('test_job_a')
    job_b.add_edge('ls', 'test_job_a')
    job_b.add_edge('test_job_a', 'pwd')
    graph, tasks = job_b.expand(job_b.graph, job_b.tasks)
    ideal_result = {
        'ls': set(['test_job_a_yes']),
        'pwd': set([]),
        'test_job_a_cd .': set(['pwd']),
        'test_job_a_ls': set(['test_job_a_cd .']),
        'test_job_a_yes': set(['test_job_a_ls'])
    }
    assert assert_graph_equality(ideal_result, graph)


@with_setup(blank_dagobah)
def test_dagobah_expand_complex_job():
    dagobah.add_job('JOB_1')
    dagobah.add_job('JOB_2')
    dagobah.add_job('JOB_3')

    job_1 = dagobah.get_job('JOB_1')
    job_1.add_task('yes', 'A')
    job_1.add_task('ls', 'B')
    job_1.add_jobtask('JOB_2', 'C')
    job_1.add_task('time', 'D')
    job_1.add_task('date', 'E')
    job_1.add_edge('A', 'C')
    job_1.add_edge('B', 'C')
    job_1.add_edge('C', 'D')
    job_1.add_edge('C', 'E')

    job_2 = dagobah.get_job('JOB_2')
    job_2.add_task('yes', 'A*')
    job_2.add_jobtask('JOB_3', 'B*')
    job_2.add_task('cd .', 'C*')
    job_2.add_task('time', 'D*')
    job_2.add_edge('A*', 'B*')
    job_2.add_edge('A*', 'C*')
    job_2.add_edge('B*', 'C*')
    job_2.add_edge('B*', 'D*')
    job_2.add_edge('C*', 'D*')

    job_3 = dagobah.get_job('JOB_3')
    job_3.add_task('yes', 'A**')
    job_3.add_task('ls -lahtr', 'B**')
    job_3.add_task('cd .', 'C**')
    job_3.add_task('date', 'D**')
    job_3.add_edge('A**', 'D**')
    job_3.add_edge('B**', 'D**')
    job_3.add_edge('C**', 'D**')

    graph, tasks = job_1.expand(job_1.graph, job_1.tasks)
    ideal_result = {
        'A': set(['C_A*']),
        'B': set(['C_A*']),
        'C_A*': set(['C_C_B*_A**', 'C_C_B*_B**', 'C_C_B*_C**', 'C_C_C*']),
        'C_C_B*_A**': set(['C_C_B*_D**']),
        'C_C_B*_B**': set(['C_C_B*_D**']),
        'C_C_B*_C**': set(['C_C_B*_D**']),
        'C_C_B*_D**': set(['C_C_C*', 'C_C_D*']),
        'C_C_C*': set(['C_C_D*']),
        'C_C_D*': set(['D', 'E']),
        'D': set([]),
        'E': set([])
    }
    assert assert_graph_equality(ideal_result, graph)
