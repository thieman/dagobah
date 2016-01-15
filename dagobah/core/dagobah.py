import logging
import json
import paramiko
import os

from .components import Scheduler, StrictJSONEncoder
from ..backend.base import BaseBackend
from .dagobah_error import DagobahError
from .job import Job
from .delegator import CommitDelegator

logger = logging.getLogger('dagobah')


class Dagobah(object):
    """ Top-level controller for all Dagobah usage.

    This is in control of all the jobs for a specific Dagobah
    instance, as well as top-level parameters such as the
    backend used for permanent storage.
    """
    JIJ_DELIM = '%_|JIJ_DELIMITER|_%'

    def __init__(self, backend=BaseBackend(), event_handler=None,
                 ssh_config=None):
        """ Construct a new Dagobah instance with a specified Backend. """
        logger.debug('Starting Dagobah instance constructor')
        self.backend = backend
        self.event_handler = event_handler
        self.dagobah_id = self.backend.get_new_dagobah_id()
        self.jobs = []
        self.created_jobs = 0
        self.scheduler = Scheduler(self)
        self.scheduler.daemon = True
        self.ssh_config = ssh_config

        self.scheduler.start()

        self.delegator = CommitDelegator(backend)
        self.delegator.commit_dagobah(self)

    def __repr__(self):
        return '<Dagobah with Backend %s>' % self.backend

    def set_backend(self, backend):
        """ Manually set backend after construction. """

        self.backend = backend
        self.delegator.backend = backend
        self.dagobah_id = self.backend.get_new_dagobah_id()

        for job in self.jobs:
            job.backend = backend
            for task in job.tasks.values():
                task.backend = backend

        self.delegator.commit_dagobah(self, cascade=True)

    def from_backend(self, dagobah_id):
        """ Reconstruct this Dagobah instance from the backend. """
        logger.debug('Reconstructing Dagobah instance from backend ' +
                     'with ID {0}'.format(dagobah_id))
        rec = self.backend.get_dagobah_json(dagobah_id)
        if not rec:
            raise DagobahError('dagobah with id %s does not exist '
                               'in backend' % dagobah_id)
        self._construct_from_json(rec)

    def _construct_from_json(self, rec):
        """ Construct this Dagobah instance from a JSON document. """

        self.delete()

        for required_key in ['dagobah_id', 'created_jobs']:
            setattr(self, required_key, rec[required_key])

        for job_json in rec.get('jobs', []):
            self._add_job_from_spec(job_json)

        self.delegator.commit_dagobah(self, cascade=True)

    def add_job_from_json(self, job_json, destructive=False):
        """ Construct a new Job from an imported JSON spec. """
        logger.debug('Importing job from JSON document: {0}'.format(job_json))
        rec = self.backend.decode_import_json(job_json)
        if destructive:
            try:
                self.delete_job(rec['name'])
            except DagobahError:  # expected if no job with this name
                pass
        self._add_job_from_spec(rec, use_job_id=False)

        self.delegator.commit_dagobah(self, cascade=True)

    def _add_job_from_spec(self, job_json, use_job_id=True):
        """ Add a single job to the Dagobah from a spec. """

        job_id = (job_json['job_id']
                  if use_job_id
                  else self.backend.get_new_job_id())
        self.add_job(str(job_json['name']), job_id)
        job = self.get_job(job_json['name'])
        if job_json.get('cron_schedule', None):
            job.schedule(job_json['cron_schedule'])

        for task in job_json.get('tasks', []):
            # If it is a jobtask, it will have a job_name
            if task.get('job_name'):
                self.add_jobtask_to_job(job, task['job_name'], str(task['name']))
            else:
                self.add_task_to_job(job,
                                     str(task['command']),
                                     str(task['name']),
                                     soft_timeout=task.get('soft_timeout', 0),
                                     hard_timeout=task.get('hard_timeout', 0),
                                     hostname=task.get('hostname', None))

        dependencies = job_json.get('dependencies', {})
        for from_node, to_nodes in dependencies.iteritems():
            for to_node in to_nodes:
                job.add_dependency(from_node, to_node)

        if job_json.get('notes', None):
            job.update_job_notes(job_json['notes'])

    def delete(self):
        """ Delete this Dagobah instance from the Backend. """
        logger.debug('Deleting Dagobah instance with ID {0}'.
                     format(self.dagobah_id))
        self.jobs = []
        self.created_jobs = 0
        self.backend.delete_dagobah(self.dagobah_id)

    def add_job(self, job_name, job_id=None):
        """ Create a new, empty Job. """
        logger.debug('Creating a new job named {0}'.format(job_name))
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
        self.delegator.commit_job(job)

    def load_ssh_conf(self):
        try:
            conf_file = open(os.path.expanduser(self.ssh_config))
            ssh_config = paramiko.SSHConfig()
            ssh_config.parse(conf_file)
            conf_file.close()
            return ssh_config
        except IOError:
            logger.warn('Tried to load SSH config but failed, probably file ' +
                        'not found')
            return None

    def get_hosts(self):
        conf = self.load_ssh_conf()

        if conf is None:
            return []

        # Please help me make this cleaner I'm in list comprehension hell

        # Explanation: the _config option contains a list of dictionaries,
        # each dictionary is a representation of a "host", complete with
        # configuration options (user, address, identityfile). The "host"
        # attribute of each dict is a list of hostnames that share the same
        # config options. For most users this contains only one entry, but
        # all are valid host choices. We filter hosts with "*" because dagobah
        # does not support wildcard matching for a task to run on all hosts.
        hosts = [item for sublist in
                 [hostnames['host'] for hostnames in conf._config]
                 for item in sublist if not '*' in item]
        return hosts

    def get_host(self, hostname):
        """ Returns a Host dict with config options, or None if none exists"""
        if hostname in self.get_hosts():
            return self.load_ssh_conf().lookup(hostname)
        logger.warn('Tried to find host with name {0}, but host not found'.
                    format(hostname))
        return None

    def get_job(self, job_name):
        """ Returns a Job by name, or None if none exists. """
        for job in self.jobs:
            if job.name == job_name:
                return job
        logger.warn('Tried to find job with name {0}, but job not found'.
                    format(job_name))
        return None

    def delete_job(self, job_name):
        """ Delete a job by name, or error out if no such job exists. """
        logger.debug('Deleting job {0}'.format(job_name))
        for idx, job in enumerate(self.jobs):
            if job.name == job_name:
                self.backend.delete_job(job.job_id)
                del self.jobs[idx]
                self.delegator.commit_dagobah(self)
                return
        raise DagobahError('no job with name %s exists' % job_name)

    def _resolve_job(self, job):
        """
        Internal convenience class that will resolve a job id string to a Job
        object if passed
        """
        if not isinstance(job, Job):
            job = self.get_job(job)

        if not job:
            raise DagobahError('job %s does not exist' % job)

        return job

    def add_task_to_job(self, job_or_job_name, task_command, task_name=None,
                        **kwargs):
        """ Add a task to a job owned by the Dagobah instance. """
        job = self._resolve_job(job_or_job_name)

        logger.debug('Adding task with command {0} to job {1}'.
                     format(task_command, job.name))

        if not job.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current " +
                               "state: %s"
                               % job.state.status)

        job.add_task(task_command, task_name, **kwargs)
        self.delegator.commit_job(job)

    def add_jobtask_to_job(self, job_or_job_name, target_job, task_name=None):
        """ Add a task to a job owned by the Dagobah instance. """

        job = self._resolve_job(job_or_job_name)
        target_job = self._resolve_job(target_job)

        logger.debug('Adding JobTask with target job {0} to parent job {1}'.
                     format(target_job.name, job.name))

        if not job.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current " +
                               "state: %s"
                               % job.state.status)

        job.add_jobtask(target_job.name, task_name)
        self.delegator.commit_job(job)

    def _name_is_available(self, job_name):
        """ Returns Boolean of whether the specified name is already in use."""
        return (False
                if [job for job in self.jobs if job.name == job_name]
                else True)

    def _serialize(self, include_run_logs=False, strict_json=False):
        """ Serialize a representation of this Dagobah object to JSON. """
        result = {'dagobah_id': self.dagobah_id,
                  'created_jobs': self.created_jobs,
                  'jobs': [job._serialize(include_run_logs=include_run_logs,
                                          strict_json=strict_json)
                           for job in self.jobs]}
        if strict_json:
            result = json.loads(json.dumps(result, cls=StrictJSONEncoder))
        return result
