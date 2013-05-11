""" Core classes for tasks and jobs (groups of tasks) """

import os
from datetime import datetime
import subprocess
import threading

from croniter import croniter

from dagobah.core.dag import DAG
from dagobah.backend.base import BaseBackend


class Dagobah(object):
    """ Top-level controller for all Dagobah usage.

    This is in control of all the jobs for a specific Dagobah
    instance, as well as top-level parameters such as the
    backend used for permanent storage.
    """

    def __init__(self, backend=BaseBackend()):
        """ Construct a new Dagobah instance with a specified Backend. """
        self.backend = backend
        self.jobs = []
        self.created_jobs = 0


    def __repr__(self):
        return '<Dagobah with Backend %s>' % self.backend


    def add_job(self, job_name):
        """ Create a new, empty Job. """
        if not self._name_is_available(job_name):
            raise KeyError('name %s is not available' % job_name)

        self.jobs.append(Job(self.backend,
                             self.created_jobs + 1,
                             job_name))
        self.created_jobs += 1


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
                del self.jobs[idx]
                return
        raise KeyError('no job with name %s exists' % job_name)


    def add_task_to_job(self, job_or_job_name, task_command, task_name=None):
        """ Add a task to a job owned by the Dagobah instance. """

        if isinstance(job_or_job_name, Job):
            job = job_or_job_name
        else:
            job = self.get_job(job_or_job_name)

        if not job:
            raise KeyError('job %s does not exist' % job_or_job_name)
        job.add_task(task_command, task_name)


    def _name_is_available(self, job_name):
        """ Returns Boolean of whether the specified name is already in use. """
        return (False
                if [job for job in self.jobs if job.name == job_name]
                else True)


    def _serialize(self):
        """ Serialize a representation of this Dagobah object to JSON. """
        return {'jobs': [job._serialize() for job in self.jobs],
                'created_jobs': self.created_jobs}


class Task(object):
    """ Handles execution and reporting for an individual process. """

    def __init__(self, parent_job, command, name):
        self.parent_job = parent_job
        self.command = command
        self.name = name

        self.process = None
        self.stdout = None
        self.stderr = None
        self.stdout_file = None
        self.stderr_file = None

        self.timer = None


    def start(self):
        """ Begin execution of this task. """
        self.stdout_file = os.tmpfile()
        self.stderr_file = os.tmpfile()
        self.process = subprocess.Popen(self.command,
                                        shell=True,
                                        stdout=self.stdout_file,
                                        stderr=self.stderr_file)
        self._start_check_timer()


    def check_complete(self):
        """ Runs completion flow for this task if it's finished. """
        if self.process.poll() is None:
            self._start_check_timer()
            return

        self.stdout, self.stderr = (self._read_temp_file(self.stdout_file),
                                    self._read_temp_file(self.stderr_file))
        for temp_file in [self.stdout_file, self.stderr_file]:
            temp_file.close()

        self._task_complete(success=True if self.process.returncode == 0 else False,
                            return_code=self.process.returncode,
                            stdout = self.stdout,
                            stderr = self.stderr,
                            complete_time = datetime.utcnow())


    def terminate(self):
        """ Send SIGTERM to the task's process. """
        if not self.process:
            raise ValueError('task does not have a running process')
        self.process.terminate()


    def kill(self):
        """ Send SIGKILL to the task's process. """
        if not self.process:
            raise ValueError('task does not have a running process')
        self.process.kill()


    def head(self, stream='stdout', num_lines=10):
        """ Head a specified stream (stdout or stderr) by num_lines. """
        target = self._map_string_to_file(stream)
        return self._head_temp_file(target, num_lines)


    def tail(self, stream='stdout', num_lines=10):
        """ Tail a specified stream (stdout or stderr) by num_lines. """
        target = self._map_string_to_file(stream)
        return self._tail_temp_file(target, num_lines)


    def get_stdout(self):
        """ Returns the entire stdout output of this process. """
        return self._read_temp_file(self.stdout_file)


    def get_stderr(self):
        """ Returns the entire stderr output of this process. """
        return self._read_temp_file(self.stderr_file)


    def _map_string_to_file(self, stream):
        if stream not in ['stdout', 'stderr']:
            raise ValueError('stream must be stdout or stderr')
        return self.stdout_file if stream == 'stdout' else self.stderr_file


    def _start_check_timer(self):
        """ Periodically checks to see if the task has completed. """
        self.timer = threading.Timer(2.5, self.check_complete)
        self.timer.daemon = True
        self.timer.start()


    def _read_temp_file(self, temp_file):
        """ Reads a temporary file for Popen stdout and stderr. """
        temp_file.seek(0)
        result = temp_file.read()
        return result


    def _head_temp_file(self, temp_file, num_lines):
        """ Returns a list of the first num_lines lines from a temp file. """
        if not isinstance(num_lines, int):
            raise TypeError('num_lines must be an integer')
        temp_file.seek(0)
        result, curr_line = [], 0
        for line in temp_file:
            curr_line += 1
            result.append(line)
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
            raise TypeError('num_lines must be an integer')

        temp_file.seek(0, os.SEEK_END)
        size = temp_file.tell()
        temp_file.seek(-1 * min(size, seek_offset), os.SEEK_END)

        result = []
        while True:
            this_line = temp_file.readline()
            if this_line == '':
                break
            result.append(this_line)
            if len(result) > num_lines:
                result.pop(0)
        return result


    def _task_complete(self, **kwargs):
        """ Performs cleanup tasks and notifies Job that the Task finished. """
        self.parent_job.completion_lock.acquire()
        self.parent_job._complete_task(self.name, **kwargs)


    def _serialize(self):
        """ Serialize a representation of this Task to a Python dict. """
        return {'command': self.command,
                'name': self.name}


class Job(DAG):
    """ Controller for a collection and graph of Task objects. """

    def __init__(self, backend, job_id, name):
        super(Job, self).__init__()

        self.backend = backend
        self.job_id = job_id
        self.name = name

        # tasks themselves aren't hashable, so we need a secondary lookup
        self.tasks = {}
        self.base_datetime = datetime.utcnow()

        self.status = None
        self.next_run = None
        self.cron_schedule = None
        self.cron_iter = None
        self.run_log = None
        self.completion_lock = threading.Lock()

        self._set_status('waiting')


    def from_backend(self):
        """ Construct this job from information in the backend. """
        raise NotImplementedError()


    def commit(self):
        """ Store metadata on this Job to the backend. """
        self.backend.commit_job(self._serialize())


    def add_task(self, command, name=None):
        """ Adds a new Task to the graph with no edges. """
        if name is None:
            name = command
        new_task = Task(self, command, name)
        self.tasks[name] = new_task
        self.add_node(name)


    def schedule(self, cron_schedule):
        """ Schedules the job to run periodically using Cron syntax. """
        self.cron_schedule = cron_schedule
        self.cron_iter = croniter(cron_schedule, self.base_datetime)
        self.next_run = self.cron_iter.get_next(datetime)


    def start(self):
        """ Begins the job by kicking off all tasks with no dependencies. """

        if self.status == 'running':
            raise ValueError('job is already running')

        if self.cron_iter:
            self.next_run = self.cron_iter.get_next(datetime)

        self.run_log = {'start_time': datetime.utcnow(),
                        'tasks': {}}
        self._set_status('running')

        for task_name in self.ind_nodes():
            self._put_task_in_run_log(task_name)
            self.tasks[task_name].start()


    def retry(self):
        """ Starts failed parts of a job from a failed state. """

        if self.status != 'failed':
            raise ValueError('can only retry a job from a failed state')

        self._set_status('running')
        self.run_log['retry_time'] = datetime.utcnow()

        for task_name, log in self.run_log['tasks'].iteritems():
            if log.get('success', True) == False:
                self.tasks[task_name].start()


    def _complete_task(self, task_name, **kwargs):
        """ Marks this task as completed. Kwargs are stored in the run log. """

        self.run_log['tasks'][task_name] = kwargs

        for node in self.downstream(task_name):
            self._start_if_ready(node)

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

        if not self._is_complete():
            self.completion_lock.release()
            return

        self._commit_run_log()

        for job, results in self.run_log['tasks'].iteritems():
            if results.get('success', False) == False:
                self._set_status('failed')
                break

        if self.status != 'failed':
            self._set_status('waiting')
            self.run_log = {}

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
        if status.lower() not in ['waiting', 'running', 'failed']:
            raise ValueError('unknown status %s' % status)
        self.status = status.lower()


    def _commit_run_log(self):
        """" Commit the current run log to the backend. """
        self.backend.commit_log(self.run_log)


    def _serialize(self):
        """ Serialize a representation of this Job to a Python dict object. """
        return {'job_id': self.job_id,
                'tasks': [task._serialize()
                          for task in self.tasks.itervalues()],
                'cron_schedule': self.cron_schedule,
                'next_run': self.next_run}
