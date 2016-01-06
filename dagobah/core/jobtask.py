import logging
import json

from copy import deepcopy

from .components import StrictJSONEncoder

logger = logging.getLogger('dagobah')


class JobTask(object):
    """ Expandable Task that references a job """
    def __init__(self, parent_job, target_job_name, task_name):
        logger.debug('Starting JobTask instance constructor with job {0}'.
                     format(target_job_name))
        self.parent_job = parent_job
        self.target_job_name = target_job_name
        self.name = task_name

        self.delegator = parent_job.delegator
        self.delegator.commit_job(self.parent_job)

    def expand(self, expanding_job=None):
        """ Expand this JobTask into a list of cloned tasks """
        logger.debug("expanding {0}".format(self.target_job_name))
        target_job = self.parent_job.parent._resolve_job(self.target_job_name)

        graph_copy = deepcopy(target_job.graph)
        tasks_copy = dict((n, t.clone()) for (n, t) in target_job.tasks.iteritems())

        return target_job.expand(graph_copy, tasks_copy)

    def _serialize(self, include_run_logs=False, strict_json=False):
        """ Serialize a representation of this Task to a Python dict. """

        result = {'name': self.name,
                  'job_name': self.target_job_name}

        if strict_json:
            result = json.loads(json.dumps(result, cls=StrictJSONEncoder))
        return result

    def clone(self):
        cloned_task = JobTask(self.parent_job, self.target_job_name, self.name)
        return cloned_task
