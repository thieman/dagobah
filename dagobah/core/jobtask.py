import logging
import json

from .components import StrictJSONEncoder
from .dagobah_error import DagobahError

logger = logging.getLogger('dagobah')


class JobTask(object):
    """ Expandable Task that references a job """
    def __init__(self, parent_job, target_job_name, task_name):
        logger.debug('Starting JobTask instance constructor with job {0}'.
                     format(target_job_name))
        self.parent_job = parent_job
        self.target_job_name = target_job_name
        self.name = task_name

    def expand(self):
        """ Expand this JobTask into a list of tasks """
        return self.parent_job.parent.get_job(
            self.target_job_name).tasks.values()

    def _serialize(self, include_run_logs=False, strict_json=False):
        """ Serialize a representation of this Task to a Python dict. """

        result = {'name': self.name,
                  'job_name': self.target_job_name}

        if strict_json:
            result = json.loads(json.dumps(result, cls=StrictJSONEncoder))
        return result
