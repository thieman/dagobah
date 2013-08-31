""" Component classes used by core classes. """

import inspect
import logging
from datetime import datetime
from collections import defaultdict
import time
import threading
import json
from bson import ObjectId


class EventHandler(object):
    """ Provides an event model for Dagobah methods.

    The Dagobah instance emits events, which can then trigger
    handlers that are registered in this class.
    """

    def __init__(self):
        self.handlers = defaultdict(list)


    def emit(self, event, event_params={}):
        try:
            methods = self.handlers.get(event, [])
            for method, args, kwargs in methods:
                argspec = inspect.getargspec(method)
                if ('event_params' in argspec.args or argspec.keywords is not None):
                    kwargs = dict(kwargs.items() +
                                  {'event_params': event_params}.items())
                method.__call__(*args, **kwargs)
        except Exception:
            logging.exception('Exception emitting event %s' % event)


    def register(self, event, method, *args, **kwargs):
        if 'event_params' in kwargs:
            raise ValueError('event_params is a reserved key')
        self.handlers[event].append((method, args, kwargs))


    def deregister(self, event, method):
        for idx, registered in enumerate(self.handlers[event]):
            if registered[0] == method:
                self.handlers[event].pop(idx)
                break


class JobState(object):
    """ Stores state and related state metadata of a current job. """

    def __init__(self):
        self.status = None

        self.perms = {'allow_start': ['waiting', 'failed'],
                      'allow_change_graph': ['waiting', 'failed'],
                      'allow_change_schedule': ['waiting', 'running', 'failed'],
                      'allow_edit_job': ['waiting', 'failed'],
                      'allow_edit_task': ['waiting', 'failed']}

        for key in self.perms.keys():
            setattr(self, key, None)


    def set_status(self, status):
        status = status.lower()
        if status not in ['waiting', 'running', 'failed']:
            raise ValueError('unknown status %s' % status)

        self.status = status
        self._set_permissions()


    def _set_permissions(self):
        for perm, states in self.perms.iteritems():
            setattr(self, perm, True if self.status in states else False)


class Scheduler(threading.Thread):
    """ Monitoring thread to kick off Jobs at their scheduled times. """

    def __init__(self, parent_dagobah):
        super(Scheduler, self).__init__()
        self.parent = parent_dagobah
        self.stopped = False

        self.last_check = datetime.utcnow()


    def __repr__(self):
        return '<Scheduler for %s>' % self.parent


    def stop(self):
        """ Stop the monitoring loop without killing the thread. """
        self.stopped = True


    def restart(self):
        """ Restart the monitoring loop. """
        self.last_check = datetime.utcnow()
        self.stopped = False


    def run(self):
        """ Continually monitors Jobs of the parent Dagobah. """
        while not self.stopped:
            now = datetime.utcnow()
            for job in self.parent.jobs:
                if not job.next_run:
                    continue
                if job.next_run >= self.last_check and job.next_run <= now:
                    if job.state.allow_start:
                        job.start()
                    else:
                        job.next_run = job.cron_iter.get_next(datetime)
            self.last_checked = now
            time.sleep(1)


class StrictJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)
