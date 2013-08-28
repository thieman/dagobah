""" Core classes for tasks and jobs (groups of tasks) """

import os
from datetime import datetime
import time
import threading
import subprocess
import paramiko
import base64
import StringIO
import select
from multiprocessing import Process, Manager

from croniter import croniter

from dagobah.core.dag import DAG
from dagobah.core.components import Scheduler, JobState
from dagobah.backend.base import BaseBackend


class DagobahError(Exception):
    pass


class Dagobah(object):
    """ Top-level controller for all Dagobah usage.

    This is in control of all the jobs for a specific Dagobah
    instance, as well as top-level parameters such as the
    backend used for permanent storage.
    """

    def __init__(self, backend=BaseBackend(), event_handler=None):
        """ Construct a new Dagobah instance with a specified Backend. """
        self.backend = backend
        self.event_handler = event_handler
        self.dagobah_id = self.backend.get_new_dagobah_id()
        self.jobs = []
        self.created_jobs = 0
        self.scheduler = Scheduler(self)
        self.scheduler.daemon = True

        self.scheduler.start()

        self.commit()


    def __repr__(self):
        return '<Dagobah with Backend %s>' % self.backend


    def set_backend(self, backend):
        """ Manually set backend after construction. """

        self.backend = backend
        self.dagobah_id = self.backend.get_new_dagobah_id()

        for job in self.jobs:
            job.backend = backend
            for task in job.tasks.values():
                task.backend = backend

        self.commit(cascade=True)


    def from_backend(self, dagobah_id):
        """ Reconstruct this Dagobah instance from the backend. """

        rec = self.backend.get_dagobah_json(dagobah_id)
        if not rec:
            raise DagobahError('dagobah with id %s does not exist in backend' %
                               dagobah_id)

        # delete current version of this Dagobah instance
        self.delete()

        for required_key in ['dagobah_id', 'created_jobs']:
            setattr(self, required_key, rec[required_key])

        for job_json in rec.get('jobs', []):

            self.add_job(str(job_json['name']), job_json['job_id'])
            job = self.get_job(job_json['name'])
            if job_json.get('cron_schedule', None):
                job.schedule(job_json['cron_schedule'])

            for task in job_json.get('tasks', []):
                self.add_task_to_job(job,
                                     str(task['command']),
                                     str(task['name']),
                                     soft_timeout=task.get('soft_timeout', 0),
                                     hard_timeout=task.get('hard_timeout', 0),
                                     task_target=task.get('task_target', None),
                                     task_target_key=task.get('task_target_key', None),
                                     task_target_password=task.get('task_target_password', None))

            dependencies = job_json.get('dependencies', {})
            for from_node, to_nodes in dependencies.iteritems():
                for to_node in to_nodes:
                    job.add_dependency(from_node, to_node)

        self.commit(cascade=True)


    def commit(self, cascade=False):
        """ Commit this Dagobah instance to the backend.

        If cascade is True, all child Jobs are commited as well.
        """
        self.backend.commit_dagobah(self._serialize())
        if cascade:
            [job.commit() for job in self.jobs]


    def delete(self):
        """ Delete this Dagobah instance from the Backend. """
        self.jobs = []
        self.created_jobs = 0
        self.backend.delete_dagobah(self.dagobah_id)


    def add_job(self, job_name, job_id=None):
        """ Create a new, empty Job. """
        if not self._name_is_available(job_name):
            raise DagobahError('name %s is not available' % job_name)

        if not job_id:
            job_id = self.backend.get_new_job_id()
            self.created_jobs += 1

        self.jobs.append(Job(self,
                             self.backend,
                             job_id,
                             job_name))

        job = self.get_job(job_name)
        job.commit()


    def get_job(self, job_name):
        """ Returns a Job by name, or None if none exists. """
        for job in self.jobs:
            if job.name == job_name:
                return job
        return None


    def delete_job(self, job_name):
        """ Delete a job by name, or error out if no such job exists. """
        for idx, job in enumerate(self.jobs):
            if job.name == job_name:
                self.backend.delete_job(job.job_id)
                del self.jobs[idx]
                return
        raise DagobahError('no job with name %s exists' % job_name)


    def add_task_to_job(self, job_or_job_name, task_command, task_name=None,
                        **kwargs):
        """ Add a task to a job owned by the Dagobah instance. """
        if isinstance(job_or_job_name, Job):
            job = job_or_job_name
        else:
            job = self.get_job(job_or_job_name)

        if not job:
            raise DagobahError('job %s does not exist' % job_or_job_name)

        if not job.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current state: %s"
                               % job.state.status)

        job.add_task(task_command, task_name, **kwargs)
        job.commit()


    def _name_is_available(self, job_name):
        """ Returns Boolean of whether the specified name is already in use. """
        return (False
                if [job for job in self.jobs if job.name == job_name]
                else True)


    def _serialize(self, include_run_logs=False):
        """ Serialize a representation of this Dagobah object to JSON. """
        return {'dagobah_id': self.dagobah_id,
                'created_jobs': self.created_jobs,
                'jobs': [job._serialize(include_run_logs=include_run_logs)
                         for job in self.jobs]}


class Job(DAG):
    """ Controller for a collection and graph of Task objects.

    Emitted events:

    job_complete: On successful completion of the job. Returns
    the current serialization of the job with run logs.
    job_failed: On failed completion of the job. Returns
    the current serialization of the job with run logs.
    """

    def __init__(self, parent, backend, job_id, name):
        super(Job, self).__init__()

        self.parent = parent
        self.backend = backend
        self.event_handler = self.parent.event_handler
        self.job_id = job_id
        self.name = name
        self.state = JobState()

        # tasks themselves aren't hashable, so we need a secondary lookup
        self.tasks = {}

        self.next_run = None
        self.cron_schedule = None
        self.cron_iter = None
        self.run_log = None
        self.completion_lock = threading.Lock()

        self._set_status('waiting')

        self.commit()


    def commit(self):
        """ Store metadata on this Job to the backend. """
        self.backend.commit_job(self._serialize())
        self.parent.commit()


    def add_task(self, command, name=None, **kwargs):
        """ Adds a new Task to the graph with no edges. """
        if not self.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current state: %s"
                               % self.state.status)

        if name is None:
            name = command
        new_task = Task(self, command, name, **kwargs)
        self.tasks[name] = new_task
        self.add_node(name)
        self.commit()


    def add_dependency(self, from_task_name, to_task_name):
        """ Add a dependency between two tasks. """

        if not self.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current state: %s"
                               % self.state.status)

        self.add_edge(from_task_name, to_task_name)
        self.commit()


    def delete_task(self, task_name):
        """ Deletes the named Task in this Job. """

        if not self.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current state: %s"
                               % self.state.status)

        if task_name not in self.tasks:
            raise DagobahError('task %s does not exist' % task_name)

        self.tasks.pop(task_name)
        self.delete_node(task_name)
        self.commit()


    def delete_dependency(self, from_task_name, to_task_name):
        """ Delete a dependency between two tasks. """

        if not self.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current state: %s"
                               % self.state.status)

        self.delete_edge(from_task_name, to_task_name)
        self.commit()


    def schedule(self, cron_schedule, base_datetime=None):
        """ Schedules the job to run periodically using Cron syntax. """

        if not self.state.allow_change_schedule:
            raise DagobahError("job's schedule cannot be changed in state: %s"
                               % self.state.status)

        if cron_schedule is None:
            self.cron_schedule = None
            self.cron_iter = None
            self.next_run = None

        else:
            if base_datetime is None:
                base_datetime = datetime.utcnow()
            self.cron_schedule = cron_schedule
            self.cron_iter = croniter(cron_schedule, base_datetime)
            self.next_run = self.cron_iter.get_next(datetime)

        self.commit()


    def start(self):
        """ Begins the job by kicking off all tasks with no dependencies. """

        if not self.state.allow_start:
            raise DagobahError('job cannot be started in its current state; ' +
                               'it is probably already running')

        is_valid, reason = self.validate()
        if not is_valid:
            raise DagobahError(reason)

        # don't increment if the job was run manually
        if self.cron_iter and datetime.utcnow() > self.next_run:
            self.next_run = self.cron_iter.get_next(datetime)

        self.run_log = {'job_id': self.job_id,
                        'name': self.name,
                        'parent_id': self.parent.dagobah_id,
                        'log_id': self.backend.get_new_log_id(),
                        'start_time': datetime.utcnow(),
                        'tasks': {}}
        self._set_status('running')

        for task in self.tasks.itervalues():
            task.reset()

        for task_name in self.ind_nodes():
            self._put_task_in_run_log(task_name)
            self.tasks[task_name].start()

        self._commit_run_log()


    def retry(self):
        """ Restarts failed tasks of a job. """

        failed_task_names = []
        for task_name, log in self.run_log['tasks'].items():
            if log.get('success', True) == False:
                failed_task_names.append(task_name)

        if len(failed_task_names) == 0:
            raise DagobahError('no failed tasks to retry')

        self._set_status('running')
        self.run_log['last_retry_time'] = datetime.utcnow()

        for task_name in failed_task_names:
            self._put_task_in_run_log(task_name)
            self.tasks[task_name].start()

        self._commit_run_log()


    def terminate_all(self):
        """ Terminate all currently running jobs. """
        for task in self.tasks.itervalues():
            if task.started_at and not task.completed_at:
                task.terminate()


    def kill_all(self):
        """ Kill all currently running jobs. """
        for task in self.tasks.itervalues():
            if task.started_at and not task.completed_at:
                task.kill()


    def edit(self, **kwargs):
        """ Change this Job's name.

        This will affect the historical data available for this
        Job, e.g. past run logs will no longer be accessible.
        """

        if not self.state.allow_edit_job:
            raise DagobahError('job cannot be edited in its current state')

        if 'name' in kwargs and isinstance(kwargs['name'], str):
            if not self.parent._name_is_available(kwargs['name']):
                raise DagobahError('new job name %s is not available' %
                                   kwargs['name'])

        for key in ['name']:
            if key in kwargs and isinstance(kwargs[key], str):
                setattr(self, key, kwargs[key])

        self.parent.commit(cascade=True)


    def edit_task(self, task_name, **kwargs):
        """ Change the name of a Task owned by this Job.

        This will affect the historical data available for this
        Task, e.g. past run logs will no longer be accessible.
        """

        if not self.state.allow_edit_task:
            raise DagobahError("tasks cannot be edited in this job's " +
                             "current state")

        if task_name not in self.tasks:
            raise DagobahError('task %s not found' % task_name)

        if 'name' in kwargs and isinstance(kwargs['name'], str):
            if kwargs['name'] in self.tasks:
                raise DagobahError('task name %s is unavailable' %
                               kwargs['name'])

        task = self.tasks[task_name]
        for key in ['name', 'command']:
            if key in kwargs and isinstance(kwargs[key], str):
                setattr(task, key, kwargs[key])

        if 'soft_timeout' in kwargs:
            task.set_soft_timeout(kwargs['soft_timeout'])

        if 'hard_timeout' in kwargs:
            task.set_hard_timeout(kwargs['hard_timeout'])

        if 'task_target' in kwargs:
            task.set_task_target(kwargs['task_target'][0])

        if 'name' in kwargs and isinstance(kwargs['name'], str):
            self.rename_edges(task_name, kwargs['name'])
            self.tasks[kwargs['name']] = task
            del self.tasks[task_name]

        self.parent.commit(cascade=True)


    def _complete_task(self, task_name, **kwargs):
        """ Marks this task as completed. Kwargs are stored in the run log. """

        self.run_log['tasks'][task_name] = kwargs

        for node in self.downstream(task_name):
            self._start_if_ready(node)

        try:
            self.backend.acquire_lock()
            self._commit_run_log()
        except:
            raise
        finally:
            self.backend.release_lock()

        if kwargs.get('success', None) == False:
            task = self.tasks[task_name]
            try:
                self.backend.acquire_lock()
                if self.event_handler:
                    self.event_handler.emit('task_failed',
                                            task._serialize(include_run_logs=True))
            except:
                raise
            finally:
                self.backend.release_lock()

        self._on_completion()


    def _put_task_in_run_log(self, task_name):
        """ Initializes the run log task entry for this task. """
        data = {'start_time': datetime.utcnow(),
                'command': self.tasks[task_name].command}
        self.run_log['tasks'][task_name] = data


    def _is_complete(self):
        """ Returns Boolean of whether the Job has completed. """
        for log in self.run_log['tasks'].itervalues():
            if 'success' not in log:  # job has not returned yet
                return False
        return True

    def _on_completion(self):
        """ Checks to see if the Job has completed, and cleans up if it has. """

        if self.state.status != 'running' or (not self._is_complete()):
            self.completion_lock.release()
            return

        for job, results in self.run_log['tasks'].iteritems():
            if results.get('success', False) == False:
                self._set_status('failed')
                try:
                    self.backend.acquire_lock()
                    if self.event_handler:
                        self.event_handler.emit('job_failed',
                                                self._serialize(include_run_logs=True))
                except:
                    raise
                finally:
                    self.backend.release_lock()
                break

        if self.state.status != 'failed':
            self._set_status('waiting')
            self.run_log = {}
            try:
                self.backend.acquire_lock()
                if self.event_handler:
                    self.event_handler.emit('job_complete',
                                            self._serialize(include_run_logs=True))
            except:
                raise
            finally:
                self.backend.release_lock()

        self.completion_lock.release()


    def _start_if_ready(self, task_name):
        """ Start this task if all its dependencies finished successfully. """
        task = self.tasks[task_name]
        dependencies = self._dependencies(task_name)
        for dependency in dependencies:
            if self.run_log['tasks'].get(dependency, {}).get('success', False) == True:
                continue
            return
        self._put_task_in_run_log(task_name)
        task.start()


    def _set_status(self, status):
        """ Enforces enum-like behavior on the status field. """
        try:
            self.state.set_status(status)
        except:
            raise DagobahError('could not set status %s' % status)


    def _commit_run_log(self):
        """" Commit the current run log to the backend. """
        self.backend.commit_log(self.run_log)


    def _serialize(self, include_run_logs=False):
        """ Serialize a representation of this Job to a Python dict object. """

        # return tasks in sorted order if graph is in a valid state
        try:
            topo_sorted = self._topological_sort()
            t = [self.tasks[task]._serialize(include_run_logs=include_run_logs)
                 for task in topo_sorted]
        except:
            t = [task._serialize(include_run_logs=include_run_logs)
                 for task in self.tasks.itervalues()]

        return {'job_id': self.job_id,
                'name': self.name,
                'parent_id': self.parent.dagobah_id,
                'tasks': t,
                'dependencies': {k: list(v)
                                 for k, v
                                 in self.graph.iteritems()},
                'status': self.state.status,
                'cron_schedule': self.cron_schedule,
                'next_run': self.next_run}


class Task(object):
    """ Handles execution and reporting for an individual process.

    Emitted events:
    task_failed: On failure of an individual task. Returns the
    current serialization of the task with run logs.
    """

    def __init__(self, parent_job, command, name,
                 soft_timeout=0, hard_timeout=0, task_target=None, task_target_key=None,
                 task_target_password=None):
        self.parent_job = parent_job
        self.backend = self.parent_job.backend
        self.event_handler = self.parent_job.event_handler
        self.command = command
        self.name = name
        self.task_target = task_target
        self.task_target_key = task_target_key
        self.task_target_password = task_target_password

        self.remote_process = None
        
        self.process = None
        self.stdout = None
        self.stderr = None
        self.stdout_file = None
        self.stderr_file = None

        self.timer = None

        self.started_at = None
        self.completed_at = None
        self.successful = None

        self.terminate_sent = False
        self.kill_sent = False

        self.set_soft_timeout(soft_timeout)
        self.set_hard_timeout(hard_timeout)

        self.parent_job.commit()

    def set_soft_timeout(self, timeout):
        if not isinstance(timeout, (int, float)) or timeout < 0:
            raise ValueError('timeouts must be non-negative numbers')
        self.soft_timeout = timeout
        self.parent_job.commit()

    def set_hard_timeout(self, timeout):
        if not isinstance(timeout, (int, float)) or timeout < 0:
            raise ValueError('timeouts must be non-negative numbers')
        self.hard_timeout = timeout
        self.parent_job.commit()

    def set_task_target(self, task_target):
        self.task_target = task_target
        self.parent_job.commit()

    def reset(self):
        """ Reset this Task to a clean state prior to execution. """

        self.stdout_file = os.tmpfile()
        self.stderr_file = os.tmpfile()

        self.started_at = None
        self.completed_at = None
        self.successful = None

        self.terminate_sent = False
        self.kill_sent = False


    def start(self):
        """ Begin execution of this task. """
        self.reset()
        if self.task_target:
            self.stdout = Manager().Value(unicode, '')
            self.stderr = Manager().Value(unicode, '')
            self.remote_exit_status = Manager().Value('i', -1)

            self.remote_process = Process(target=self.remote_ssh, args=[
                                          self.stdout, self.stderr, self.remote_exit_status])
            self.remote_process.start()
        else:
            self.process = subprocess.Popen(self.command,
                                            shell=True,
                                            stdout=self.stdout_file,
                                            stderr=self.stderr_file)

        self.started_at = datetime.utcnow()
        self._start_check_timer()


    def remote_ssh(self, stdout, stderr, exit_status):
        try:
            private_key = StringIO.StringIO(str(self.task_target_key))
            key = paramiko.DSSKey.from_private_key(private_key)
        except paramiko.SSHException:
            private_key = StringIO.StringIO(str(self.task_target_key))
            key = paramiko.RSAKey.from_private_key(private_key)

        username_host = self.task_target.split("@")

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.task_target_password:
            client.connect(username_host[1], username=username_host[
                           0], password=self.task_target_password)
        else:
            client.connect(
                username_host[1], username=username_host[0], pkey=key)

        stdin_remote, stdout_remote, stderr_remote = client.exec_command(
            self.command)

        stdout.value = "".join(stdout_remote.readlines())
        if stderr_remote:
            stderr.value = "".join(stderr_remote.readlines())
        exit_status.value = stdout_remote.channel.recv_exit_status()


    def check_complete(self):
        """ Runs completion flow for this task if it's finished. """
        if self.remote_process and self.remote_process.is_alive():
            self._start_check_timer()
            return

        if self.process and self.process.poll() is None:
            # timeout check
            if (self.soft_timeout != 0 and
                (datetime.utcnow() - self.started_at).seconds >= self.soft_timeout and
                not self.terminate_sent):
                self.terminate()

            if (self.hard_timeout != 0 and
                (datetime.utcnow() - self.started_at).seconds >= self.hard_timeout and
                not self.kill_sent):
                self.kill()

            self._start_check_timer()
            return

        if self.remote_process:
            return_code = self.remote_exit_status.value
            self.stdout = self.stdout.value
            self.stderr = self.stderr.value
        else:
            return_code = self.process.returncode
            self.stdout, self.stderr = (self._read_temp_file(self.stdout_file),
                                        self._read_temp_file(self.stderr_file))
            for temp_file in [self.stdout_file, self.stderr_file]:
                temp_file.close()

        if self.terminate_sent:
            self.stderr += '\nDAGOBAH SENT SIGTERM TO THIS PROCESS\n'
        if self.kill_sent:
            self.stderr += '\nDAGOBAH SENT SIGKILL TO THIS PROCESS\n'

        self.stdout_file = None
        self.stderr_file = None

        self._task_complete(success=True if return_code == 0 else False,
                            return_code=return_code,
                            stdout = self.stdout,
                            stderr = self.stderr,
                            complete_time = datetime.utcnow())


    def terminate(self):
        """ Send SIGTERM to the task's process. """
        if self.remote_process:
            self.remote_process.terminate()
            return
        if not self.process:
            raise DagobahError('task does not have a running process')
        self.terminate_sent = True
        self.process.terminate()


    def kill(self):
        """ Send SIGKILL to the task's process. """
        if self.remote_process:
            self.remote_process.terminate()
            return

        if not self.process:
            raise DagobahError('task does not have a running process')
        self.kill_sent = True
        self.process.kill()


    def head(self, stream='stdout', num_lines=10):
        """ Head a specified stream (stdout or stderr) by num_lines. """
        target = self._map_string_to_file(stream)
        if not target:  # no current temp file
            last_run = self.backend.get_latest_run_log(self.parent_job.job_id,
                                                       self.name)
            if not last_run:
                return None
            return self._head_string(last_run['tasks'][self.name][stream],
                                     num_lines)
        else:
            return self._head_temp_file(target, num_lines)


    def tail(self, stream='stdout', num_lines=10):
        """ Tail a specified stream (stdout or stderr) by num_lines. """
        target = self._map_string_to_file(stream)
        if not target:  # no current temp file
            last_run = self.backend.get_latest_run_log(self.parent_job.job_id,
                                                       self.name)
            if not last_run:
                return None
            return self._tail_string(last_run['tasks'][self.name][stream],
                                     num_lines)
        else:
            return self._tail_temp_file(target, num_lines)


    def get_stdout(self):
        """ Returns the entire stdout output of this process. """
        return self._read_temp_file(self.stdout_file)


    def get_stderr(self):
        """ Returns the entire stderr output of this process. """
        return self._read_temp_file(self.stderr_file)


    def _map_string_to_file(self, stream):
        if stream not in ['stdout', 'stderr']:
            raise DagobahError('stream must be stdout or stderr')
        return self.stdout_file if stream == 'stdout' else self.stderr_file


    def _start_check_timer(self):
        """ Periodically checks to see if the task has completed. """
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(2.5, self.check_complete)
        self.timer.daemon = True
        self.timer.start()


    def _read_temp_file(self, temp_file):
        """ Reads a temporary file for Popen stdout and stderr. """
        temp_file.seek(0)
        result = temp_file.read()
        return result


    def _head_string(self, in_str, num_lines):
        """ Returns a list of the first num_lines lines from a string. """
        return in_str.split('\n')[:num_lines]


    def _tail_string(self, in_str, num_lines):
        """ Returns a list of the last num_lines lines from a string. """
        return in_str.split('\n')[-1 * num_lines :]


    def _head_temp_file(self, temp_file, num_lines):
        """ Returns a list of the first num_lines lines from a temp file. """
        if not isinstance(num_lines, int):
            raise DagobahError('num_lines must be an integer')
        temp_file.seek(0)
        result, curr_line = [], 0
        for line in temp_file:
            curr_line += 1
            result.append(line.strip())
            if curr_line >= num_lines:
                break
        return result


    def _tail_temp_file(self, temp_file, num_lines, seek_offset=10000):
        """ Returns a list of the last num_lines lines from a temp file.

        This works by first moving seek_offset chars back from the end of
        the file, then attempting to tail the file from there. It is
        possible that fewer than num_lines will be returned, even if the
        file has more total lines than num_lines.
        """

        if not isinstance(num_lines, int):
            raise DagobahError('num_lines must be an integer')

        temp_file.seek(0, os.SEEK_END)
        size = temp_file.tell()
        temp_file.seek(-1 * min(size, seek_offset), os.SEEK_END)

        result = []
        while True:
            this_line = temp_file.readline()
            if this_line == '':
                break
            result.append(this_line.strip())
            if len(result) > num_lines:
                result.pop(0)
        return result


    def _task_complete(self, **kwargs):
        """ Performs cleanup tasks and notifies Job that the Task finished. """
        self.parent_job.completion_lock.acquire()
        self.completed_at = datetime.utcnow()
        self.successful = kwargs.get('success', None)
        self.parent_job._complete_task(self.name, **kwargs)


    def _serialize(self, include_run_logs=False):
        """ Serialize a representation of this Task to a Python dict. """

        result = {'command': self.command,
                  'name': self.name,
                  'started_at': self.started_at,
                  'completed_at': self.completed_at,
                  'success': self.successful,
                  'soft_timeout': self.soft_timeout,
                  'hard_timeout': self.hard_timeout,
                  'task_target': self.task_target,
                  'task_target_key': self.task_target_key,
                  'task_target_password': self.task_target_password }

        if include_run_logs:
            last_run = self.backend.get_latest_run_log(self.parent_job.job_id,
                                                       self.name)
            if last_run:
                run_log = last_run.get('tasks', {}).get(self.name, {})
                if run_log:
                    result['run_log'] = run_log

        return result
