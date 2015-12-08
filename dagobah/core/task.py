import os
from datetime import datetime
import threading
import subprocess
import json
import paramiko
import logging

from .components import StrictJSONEncoder
from .dagobah_error import DagobahError

logger = logging.getLogger('dagobah')


class Task(object):
    """ Handles execution and reporting for an individual process.

    Emitted events:
    task_failed: On failure of an individual task. Returns the
    current serialization of the task with run logs.
    """

    def __init__(self, parent_job, command, name,
                 soft_timeout=0, hard_timeout=0, hostname=None):
        logger.debug('Starting Task instance constructor with name {0}'.
                     format(name))
        self.parent_job = parent_job
        self.backend = self.parent_job.backend
        self.event_handler = self.parent_job.event_handler
        self.command = command
        self.name = name
        self.hostname = hostname

        self.delegator = parent_job.delegator
        self.delegator.commit_job(self.parent_job)

        self.remote_channel = None
        self.process = None
        self.stdout = ""
        self.stderr = ""
        self.stdout_file = None
        self.stderr_file = None

        self.timer = None

        self.started_at = None
        self.completed_at = None
        self.successful = None

        self.terminate_sent = False
        self.kill_sent = False
        self.remote_failure = False

        self.set_soft_timeout(soft_timeout)
        self.set_hard_timeout(hard_timeout)

    def set_soft_timeout(self, timeout):
        logger.debug('Task {0} setting soft timeout'.format(self.name))
        if not isinstance(timeout, (int, float)) or timeout < 0:
            raise ValueError('timeouts must be non-negative numbers')
        self.soft_timeout = timeout
        self.delegator.commit_job(self.parent_job)

    def set_hard_timeout(self, timeout):
        logger.debug('Task {0} setting hard timeout'.format(self.name))
        if not isinstance(timeout, (int, float)) or timeout < 0:
            raise ValueError('timeouts must be non-negative numbers')
        self.hard_timeout = timeout
        self.delegator.commit_job(self.parent_job)

    def set_hostname(self, hostname):
        logger.debug('Task {0} setting hostname'.format(self.name))
        self.hostname = hostname
        self.delegator.commit_job(self.parent_job)

    def reset(self):
        """ Reset this Task to a clean state prior to execution. """

        logger.debug('Resetting task {0}'.format(self.name))

        self.stdout_file = os.tmpfile()
        self.stderr_file = os.tmpfile()

        self.stdout = ""
        self.stderr = ""

        self.started_at = None
        self.completed_at = None
        self.successful = None

        self.terminate_sent = False
        self.kill_sent = False
        self.remote_failure = False

    def start(self):
        """ Begin execution of this task. """
        logger.info('Starting task {0}'.format(self.name))
        self.reset()
        if self.hostname:
            host = self.parent_job.parent.get_host(self.hostname)
            if host:
                self.remote_ssh(host)
            else:
                self.remote_failure = True
        else:
            self.process = subprocess.Popen(self.command,
                                            shell=True,
                                            env=os.environ.copy(),
                                            stdout=self.stdout_file,
                                            stderr=self.stderr_file)

        self.started_at = datetime.utcnow()
        self._start_check_timer()

    def remote_ssh(self, host):
        """ Execute a command on SSH. Takes a paramiko host dict """
        logger.info('Starting remote execution of task {0} on host {1}'.
                    format(self.name, host['hostname']))
        try:
            self.remote_client = paramiko.SSHClient()
            self.remote_client.load_system_host_keys()
            self.remote_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
            self.remote_client.connect(host['hostname'], username=host['user'],
                                       key_filename=host['identityfile'][0],
                                       timeout=82800)
            transport = self.remote_client.get_transport()
            transport.set_keepalive(10)

            self.remote_channel = transport.open_session()
            self.remote_channel.get_pty()
            self.remote_channel.exec_command(self.command)
        except Exception as e:
            logger.warn('Exception encountered in remote task execution')
            self.remote_failure = True
            self.stderr += 'Exception when trying to SSH related to: '
            self.stderr += '{0}: {1}\n"'.format(type(e).__name__, str(e))
            self.stderr += 'Was looking for host "{0}"\n'.format(str(host))
            self.stderr += 'Found in config:\n'
            self.stderr += 'host: "{0}"\n'.format(str(host))
            self.stderr += 'hostname: "{0}"\n'.format(str(host.get('hostname')))
            self.stderr += 'user: "{0}"\n'.format(str(host.get('user')))
            self.stderr += 'identityfile: "{0}"\n'.format(str(host.get('identityfile')))
            self.remote_client.close()

    def check_complete(self):
        """ Runs completion flow for this task if it's finished. """
        logger.debug('Running check_complete for task {0}'.format(self.name))

        # Tasks not completed
        if self.remote_not_complete() or self.local_not_complete():
            self._start_check_timer()
            return

        return_code = self.completed_task()

        # Handle task errors
        if self.terminate_sent:
            self.stderr += '\nDAGOBAH SENT SIGTERM TO THIS PROCESS\n'
        if self.kill_sent:
            self.stderr += '\nDAGOBAH SENT SIGKILL TO THIS PROCESS\n'
        if self.remote_failure:
            return_code = -1
            self.stderr += '\nAn error occurred with the remote machine.\n'

        self.stdout_file = None
        self.stderr_file = None

        self._task_complete(success=True if return_code == 0 else False,
                            return_code=return_code,
                            stdout=self.stdout,
                            stderr=self.stderr,
                            start_time=self.started_at,
                            complete_time=datetime.utcnow())

    def remote_not_complete(self):
        """
        Returns True if this task is on a remote channel, and on a remote
        machine, False if it is either not remote or not completed
        """
        if self.remote_channel and not self.remote_channel.exit_status_ready():
            self._timeout_check()
            # Get some stdout/std error
            if self.remote_channel.recv_ready():
                self.stdout += self.remote_channel.recv(1024)
            if self.remote_channel.recv_stderr_ready():
                self.stderr += self.remote_channel.recv_stderr(1024)
            return True
        return False

    def local_not_complete(self):
        """ Returns True if task is local and not completed"""
        if self.process and self.process.poll() is None:
            self._timeout_check()
            return True
        return False

    def completed_task(self):
        """ Handle wrapping up a completed task, local or remote"""
        # If its remote and finished running
        if self.remote_channel and self.remote_channel.exit_status_ready():
            # Collect all remaining stdout/stderr
            while self.remote_channel.recv_ready():
                self.stdout += self.remote_channel.recv(1024)
            while self.remote_channel.recv_stderr_ready():
                self.stderr += self.remote_channel.recv_stderr(1024)
            return self.remote_channel.recv_exit_status()
        # Otherwise check for finished local command
        elif self.process:
            self.stdout, self.stderr = (self._read_temp_file(self.stdout_file),
                                        self._read_temp_file(self.stderr_file))
            for temp_file in [self.stdout_file, self.stderr_file]:
                temp_file.close()
            return self.process.returncode

    def terminate(self):
        """ Send SIGTERM to the task's process. """
        logger.info('Sending SIGTERM to task {0}'.format(self.name))
        if hasattr(self, 'remote_client') and self.remote_client is not None:
            self.terminate_sent = True
            self.remote_client.close()
            return
        if not self.process:
            raise DagobahError('task does not have a running process')
        self.terminate_sent = True
        self.process.terminate()

    def kill(self):
        """ Send SIGKILL to the task's process. """
        logger.info('Sending SIGKILL to task {0}'.format(self.name))
        if hasattr(self, 'remote_client') and self.remote_client is not None:
            self.kill_sent = True
            self.remote_client.close()
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

    def _timeout_check(self):
        logger.debug('Running timeout check for task {0}'.format(self.name))
        if (self.soft_timeout != 0 and
            (datetime.utcnow() - self.started_at).seconds >= self.soft_timeout
                and not self.terminate_sent):
            self.terminate()

        if (self.hard_timeout != 0 and
            (datetime.utcnow() - self.started_at).seconds >= self.hard_timeout
                and not self.kill_sent):
            self.kill()

    def get_run_log_history(self):
        return self.backend.get_run_log_history(self.parent_job.job_id,
                                                self.name)

    def get_run_log(self, log_id):
        return self.backend.get_run_log(self.parent_job.job_id, self.name,
                                        log_id)

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
        return in_str.split('\n')[-1 * num_lines:]

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
        logger.debug('Running _task_complete for task {0}'.format(self.name))
        with self.parent_job.completion_lock:
            self.completed_at = datetime.utcnow()
            self.successful = kwargs.get('success', None)
            self.parent_job._complete_task(self.name, **kwargs)

    def _serialize(self, include_run_logs=False, strict_json=False):
        """ Serialize a representation of this Task to a Python dict. """

        result = {'command': self.command,
                  'name': self.name,
                  'started_at': self.started_at,
                  'completed_at': self.completed_at,
                  'success': self.successful,
                  'soft_timeout': self.soft_timeout,
                  'hard_timeout': self.hard_timeout,
                  'hostname': self.hostname}

        if include_run_logs:
            last_run = self.backend.get_latest_run_log(self.parent_job.job_id,
                                                       self.name)
            if last_run:
                run_log = last_run.get('tasks', {}).get(self.name, {})
                if run_log:
                    result['run_log'] = run_log

        if strict_json:
            result = json.loads(json.dumps(result, cls=StrictJSONEncoder))
        return result

    def clone(self):
        cloned_task = Task(self.parent_job, self.command, self.name,
                           soft_timeout=self.soft_timeout,
                           hard_timeout=self.hard_timeout,
                           hostname=self.hostname)
        return cloned_task
