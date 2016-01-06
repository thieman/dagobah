import logging
from datetime import datetime
import threading
import json

from croniter import croniter
from copy import deepcopy
from collections import defaultdict

from dag import DAG
from .components import JobState, StrictJSONEncoder
from .task import Task
from .dagobah_error import DagobahError
from .jobtask import JobTask

logger = logging.getLogger('dagobah')


class Job(DAG):
    """ Controller for a collection and graph of Task objects.

    Emitted events:

    job_complete: On successful completion of the job. Returns
    the current serialization of the job with run logs.
    job_failed: On failed completion of the job. Returns
    the current serialization of the job with run logs.
    """

    def __init__(self, parent, backend, job_id, name):
        logger.debug('Starting Job instance constructor with name {0}'.
                     format(name))
        super(Job, self).__init__()

        self.parent = parent
        self.backend = backend
        self.event_handler = self.parent.event_handler
        self.job_id = job_id
        self.name = name
        self.state = JobState()
        self.JIJ_DELIM = self.parent.JIJ_DELIM

        # tasks themselves aren't hashable, so we need a secondary lookup
        self.tasks = {}

        self.next_run = None
        self.cron_schedule = None
        self.cron_iter = None
        self.run_log = None
        self.completion_lock = threading.Lock()
        self.notes = None

        self.snapshot = None
        self.tasks_snapshot = None

        self._set_status('waiting')

        self.delegator = parent.delegator
        self.delegator.commit_job(self)

    def _check_mutability(self):
        """ Check if graph is immutable before running """
        if not self.state.allow_change_graph:
            raise DagobahError("job's graph is immutable in its current " +
                               "state: %s" % self.state.status)

    def add_task(self, command, name=None, **kwargs):
        """ Adds a new Task to the graph with no edges. """

        logger.debug('Adding task with command {0} to job {1}'.
                     format(command, self.name))
        self._check_mutability()

        if name is None:
            name = command
        new_task = Task(self, command, name, **kwargs)
        self.tasks[name] = new_task
        self.add_node(name)
        self.delegator.commit_job(self)

    def add_jobtask(self, job_name, task_name=None):
        logger.debug('Adding JobTask named {0}, referencing {1}'.
                     format(task_name, job_name))
        self._check_mutability()

        if task_name is None:
            task_name = job_name
        new_jobtask = JobTask(self, job_name, task_name)
        self.tasks[task_name] = new_jobtask
        self.add_node(task_name)
        self.delegator.commit_job(self)

    def add_dependency(self, from_task_name, to_task_name):
        """ Add a dependency between two tasks. """

        logger.debug('Adding dependency from {0} to {1}'.format(from_task_name,
                                                                to_task_name))
        self._check_mutability()
        self.add_edge(from_task_name, to_task_name)
        self.delegator.commit_job(self)

    def delete_task(self, task_name):
        """ Deletes the named Task in this Job. """

        logger.debug('Deleting task {0}'.format(task_name))
        self._check_mutability()

        if task_name not in self.tasks:
            raise DagobahError('task %s does not exist' % task_name)

        self.tasks.pop(task_name)
        self.delete_node(task_name)
        self.delegator.commit_job(self)

    def delete_dependency(self, from_task_name, to_task_name):
        """ Delete a dependency between two tasks. """

        logger.debug('Deleting dependency from {0} to {1}'.
                     format(from_task_name, to_task_name))
        self._check_mutability()
        self.delete_edge(from_task_name, to_task_name)
        self.delegator.commit_job(self)

    def schedule(self, cron_schedule, base_datetime=None):
        """ Schedules the job to run periodically using Cron syntax. """

        logger.debug('Scheduling job {0} with cron schedule {1}'.
                     format(self.name, cron_schedule))
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

        logger.debug('Determined job {0} next run of {1}'.
                     format(self.name, self.next_run))
        self.delegator.commit_job(self)

    def start(self):
        """ Begins the job by kicking off all tasks with no dependencies. """

        logger.info('Job {0} starting job run'.format(self.name))
        if not self.state.allow_start:
            raise DagobahError('job cannot be started in its current state; ' +
                               'it is probably already running')

        self.initialize_snapshot()

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

        logger.debug('Job {0} resetting all tasks prior to start'.
                     format(self.name))
        for task in self.tasks_snapshot.itervalues():
            task.reset()

        logger.debug('Job {0} seeding run logs'.format(self.name))
        for task_name in self.ind_nodes(self.snapshot):
            self._put_task_in_run_log(task_name)
            self.tasks_snapshot[task_name].start()

        self.delegator.commit_run_log(self)

    def retry(self):
        """ Restarts failed tasks of a job. """

        logger.info('Job {0} retrying all failed tasks'.format(self.name))
        self.initialize_snapshot()

        failed_task_names = []
        for task_name, log in self.run_log['tasks'].items():
            if log.get('success', True) is False:
                failed_task_names.append(task_name)

        if len(failed_task_names) == 0:
            raise DagobahError('no failed tasks to retry')

        self._set_status('running')
        self.run_log['last_retry_time'] = datetime.utcnow()

        logger.debug('Job {0} seeding run logs'.format(self.name))
        for task_name in failed_task_names:
            self._put_task_in_run_log(task_name)
            self.tasks[task_name].start()

        self.delegator.commit_run_log(self)

    def terminate_all(self):
        """ Terminate all currently running tasks. """
        logger.info('Job {0} terminating all currently running tasks'.
                    format(self.name))
        for task in self.tasks.itervalues():
            if task.started_at and not task.completed_at:
                task.terminate()

    def kill_all(self):
        """ Kill all currently running jobs. """
        logger.info('Job {0} killing all currently running tasks'.
                    format(self.name))
        for task in self.tasks.itervalues():
            if task.started_at and not task.completed_at:
                task.kill()

    def edit(self, **kwargs):
        """ Change this Job's name.

        This will affect the historical data available for this
        Job, e.g. past run logs will no longer be accessible.
        """

        logger.debug('Job {0} changing name to {1}'.format(self.name,
                                                           kwargs.get('name')))
        if not self.state.allow_edit_job:
            raise DagobahError('job cannot be edited in its current state')

        if 'name' in kwargs and isinstance(kwargs['name'], str):
            if not self.parent._name_is_available(kwargs['name']):
                raise DagobahError('new job name %s is not available' %
                                   kwargs['name'])

        for key in ['name']:
            if key in kwargs and isinstance(kwargs[key], str):
                setattr(self, key, kwargs[key])

        self.delegator.commit_dagobah(self.parent, cascade=True)

    def update_job_notes(self, notes):
        logger.debug('Job {0} updating notes'.format(self.name))
        if not self.state.allow_edit_job:
            raise DagobahError('job cannot be edited in its current state')

        setattr(self, 'notes', notes)

        self.delegator.commit_dagobah(self.parent, cascade=True)

    def edit_task(self, task_name, **kwargs):
        """ Change the name of a Task owned by this Job.

        This will affect the historical data available for this
        Task, e.g. past run logs will no longer be accessible.
        """

        logger.debug('Job {0} editing task {1}'.format(self.name, task_name))
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

        if 'hostname' in kwargs:
            task.set_hostname(kwargs['hostname'])

        if 'name' in kwargs and isinstance(kwargs['name'], str):
            self.rename_edges(task_name, kwargs['name'])
            self.tasks[kwargs['name']] = task
            del self.tasks[task_name]

        self.delegator.commit_dagobah(self.parent, cascade=True)

    def _complete_task(self, task_name, **kwargs):
        """ Marks this task as completed. Kwargs are stored in the run log. """

        logger.debug('Job "{0}" marking task "{1}" as completed'
                     .format(self.name, task_name))
        self.run_log['tasks'][task_name] = kwargs

        for node in self.downstream(task_name, self.snapshot):
            self._start_if_ready(node)

        try:
            self.backend.acquire_lock()
            self.delegator.commit_run_log(self)
        except:
            logger.exception("Error in handling events.")
        finally:
            self.backend.release_lock()

        if kwargs.get('success', None) is False:
            task = self.tasks[task_name]
            try:
                self.backend.acquire_lock()
                if self.event_handler:
                    self.event_handler.emit('task_failed',
                                            task._serialize(
                                                include_run_logs=True))
            except:
                logger.exception("Error in handling events.")
            finally:
                self.backend.release_lock()

        self._on_completion()

    def _put_task_in_run_log(self, task_name):
        """ Initializes the run log task entry for this task. """
        logger.debug('Job {0} initializing run log entry for task {1}'.
                     format(self.name, task_name))
        data = {'start_time': datetime.utcnow(),
                'command': self.tasks_snapshot[task_name].command}
        self.run_log['tasks'][task_name] = data

    def _is_complete(self):
        """ Returns Boolean of whether the Job has completed. """
        for log in self.run_log['tasks'].itervalues():
            if 'success' not in log:  # job has not returned yet
                return False
        return True

    def _on_completion(self):
        """ Checks to see if the Job has completed, and cleans up if it has."""

        logger.debug('Job {0} running _on_completion check'.format(self.name))
        if self.state.status != 'running' or (not self._is_complete()):
            return

        for job, results in self.run_log['tasks'].iteritems():
            if results.get('success', False) is False:
                self._set_status('failed')
                try:
                    self.backend.acquire_lock()
                    if self.event_handler:
                        self.event_handler.emit('job_failed',
                                                self._serialize(
                                                    include_run_logs=True))
                except:
                    logger.exception("Error in handling events.")
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
                                            self._serialize(
                                                include_run_logs=True))
            except:
                logger.exception("Error in handling events.")
            finally:
                self.backend.release_lock()

        self.destroy_snapshot()

    def _start_if_ready(self, task_name):
        """ Start this task if all its dependencies finished successfully. """
        logger.debug('Job {0} running _start_if_ready for task {1}'.
                     format(self.name, task_name))
        task = self.tasks_snapshot[task_name]
        dependencies = self._dependencies(task_name, self.snapshot)
        for dependency in dependencies:
            if self.run_log['tasks'].get(dependency, {}).get(
                    'success', False) is True:
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

    def _serialize(self, include_run_logs=False, strict_json=False, use_snapshot=False):
        """
        Serialize a representation of this Job to a Python dict object.

        Optional Arguments:
            include_run_logs -- Include run logs in JSON result
            strict_json -- use strict_json encoder
            use_snapshot -- If there is a snapshot, use it to display task info
        """
        tasks = self.tasks
        graph = self.graph
        if use_snapshot and self.snapshot and self.tasks_snapshot:
            graph = self.snapshot
            tasks = self.tasks_snapshot

        # return tasks in sorted order if graph is in a valid state
        try:
            topo_sorted = self.topological_sort(graph)
            t = [tasks[task]._serialize(include_run_logs=include_run_logs,
                                             strict_json=strict_json)
                 for task in topo_sorted]
        except:
            t = [task._serialize(include_run_logs=include_run_logs,
                                 strict_json=strict_json)
                 for task in tasks.itervalues()]

        dependencies = {}
        for k, v in graph.iteritems():
            dependencies[k] = list(v)

        result = {'job_id': self.job_id,
                  'name': self.name,
                  'parent_id': self.parent.dagobah_id,
                  'tasks': t,
                  'dependencies': dependencies,
                  'status': self.state.status,
                  'cron_schedule': self.cron_schedule,
                  'next_run': self.next_run,
                  'notes': self.notes}

        if strict_json:
            result = json.loads(json.dumps(result, cls=StrictJSONEncoder))
        return result

    def _implements_function(self, obj, function):
        """ Checks for the existence of a method """
        if not (hasattr(obj, function) and
                callable(getattr(obj, function))):
            return False
        return True

    def implements_expandable(self, obj):
        """ Checks for methods required to expand a task """
        return self._implements_function(obj, 'expand')

    def implements_runnable(self, obj):
        """ Checks methods required to run a task. More methods to be added """
        return self._implements_function(obj, 'start')

    def initialize_snapshot(self):
        """ Copy the DAG and task list, validate, verify and expand """
        logger.debug('Initializing DAG snapshot for job {0}'.format(self.name))
        if self.snapshot is not None or self.tasks_snapshot is not None:
            logging.warn("Attempting to initialize DAG snapshot without " +
                         "first destroying old snapshot.")

        snapshot_to_validate = deepcopy(self.graph)

        is_valid, reason = self.validate(snapshot_to_validate)
        if not is_valid:
            raise DagobahError(reason)

        verified = self.verify()

        if not verified:
            raise DagobahError("Job has a cycle, possibly within another Job "
                               + "reference")

        # Due to thread locks in underlying tasks, they cannot be deepcopy'd
        tasks_copy = dict((n, t.clone()) for (n, t) in self.tasks.iteritems())
        self.snapshot, self.tasks_snapshot = self.expand(snapshot_to_validate,
                                                         tasks_copy)


    def expand(self, graph, tasks):
        """
        Given a graph and a list of tasks, return the expanded version of them.

        The process acts recursively, a general overview:
            * Starting with ind_nodes, traverse the graph
            * If the node is expandable:
                * Call expand on the JobTask, save expanded version "Expanded"
                * Connect all parents of this JobTask to the ind_nodes of
                  "Expanded"
                * Get a list of all downstream nodes in "Expanded" that don't
                  have a downstream node, and add edges to all downstream
                  nodes of the JobTask.
            * Delete JobTask from graph/task list
            * Add the downstreams of the deleted JobTask to traversal queue
        """
        logger.debug("Starting job expansion for {0}".format(self.name))

        traversal_queue = self.ind_nodes(graph)
        already_expanded = []
        while traversal_queue:
            task = traversal_queue.pop()
            if (not self.implements_expandable(tasks[task]) or
                    task in already_expanded):
                traversal_queue.extend(self.downstream(task, graph))
                continue

            already_expanded.append(task)
            expanded_graph, expanded_tasks = tasks[task].expand()

            # Empty Job expansion
            if not expanded_graph:
                pred = self.predecessors(task, graph)
                children = self.downstream(task, graph)
                [self.add_edge(p, c, graph) for p in pred for c in children]
                self.delete_node(task, graph)
                tasks.pop(task)
                continue

            # Prepend all expanded task names with "<jobname>" and delimiter to
            # mitigate task name conflicts
            renamed_tasks = {}
            for t in expanded_tasks:
                new_name = "{0}{1}{2}".format(task, self.JIJ_DELIM, t)
                renamed_tasks[new_name] = expanded_tasks[t]
                renamed_tasks[new_name].name = new_name

                expanded_graph[new_name] = expanded_graph.pop(t)
                for node, edges in expanded_graph.iteritems():
                    if t in edges:
                        edges.remove(t)
                        edges.add(new_name)
            expanded_tasks = renamed_tasks

            # Merge node and edge dictionaries (creating 2 unconnected DAGs,
            # in one graph)
            final_dict = defaultdict(set)
            for key, value in graph.iteritems():
                final_dict[key] = value
            for key, value in expanded_graph.iteritems():
                final_dict[key].update(value)
            graph = dict(final_dict)

            # Add new tasks to task dictionary
            for key, value in expanded_tasks.iteritems():
                if key in tasks:
                    raise DagobahError("Naming conflict in job expansion")
                tasks[key] = value

            # Add edges between predecessors and start nodes
            predecessors = self.predecessors(task, graph)
            start_nodes = self.ind_nodes(expanded_graph)
            [self.add_edge(p, s, graph) for p in predecessors
             for s in start_nodes]

            # Add edges between the final downstreams and the child nodes
            final_tasks = self.all_leaves(expanded_graph)
            children = self.downstream(task, graph)
            [self.add_edge(f, c, graph) for f in final_tasks for c in children]

            # add children to traversal queue and delete old reference
            traversal_queue.extend(children)
            self.delete_node(task, graph)
            tasks.pop(task)

        # Always do parental reassignment to the most current job, this
        # guarantees that the main/final job is every task's parent
        for t in tasks.itervalues():
            t.parent_job = self

        return graph, tasks

    def destroy_snapshot(self):
        """ Destroy active copy of the snapshot """
        logger.debug('Destroying DAG snapshot for job {0}'.format(self.name))
        self.snapshot = None
        self.tasks_snapshot = None

    def verify(self, context=None):
        """
        Verify that the job has no cycles where a JobTask circularly
        references another JobTask so that we know we can safely snapshot
        the DAG.

        Returns:
            bool -- indicating successful verification with True.

        Raises:
            DagobahException -- when a JobTask references a non-existent job.

        Explanation:
            1. If the current job is in the context, the job is not valid
            2. Perform a topological sort without expanding tasks (current job
               is acyclic)
            3. Traverse nodes in topological order. For each node that is a
               Job, run verify on that node, passing in the current context.
        """
        # Check if job is not in current context, then add it
        logger.debug("Verifying DAG for {0}".format(self.name))
        if context is None:
            logger.debug("No context set, using empty set")
            context = set()
        if self.name in context:
            logger.warn("Cycle detected: Job {0} already in context: {1}"
                        .format(self.name, str(context)))
            return False
        context.add(self.name)
        logger.debug("Context is now {0}".format(context))

        # Topological sort verifies DAG is acyclic before digging deeper
        topo_sorted = [self.tasks[t] for t in self.topological_sort()]

        # Traverse nodes and verify any JobTasks
        logger.debug("Traversing topologically sorted tasks.")
        for task in topo_sorted:
            if not self.implements_expandable(task):
                continue

            logger.debug("Found expandable task: {0} with target job {1}"
                         .format(task.name, task.target_job_name))
            cur_job = self.parent._resolve_job(task.target_job_name)
            if not cur_job:
                raise DagobahError("Job with name {0} doesn't exist."
                                   .format(task.target_job_name))
            if cur_job.name in context:
                logging.warn("Cycle detected in Job: {0}."
                             .format(task.target_job_name))
                return False

            # Verify this job has no internal cycles, or references to jobs
            # in the current context
            verified = cur_job.verify(context)
            if not verified:
                logger.warn("Cycle or error detected in sub-job: {0}"
                            .format(cur_job))
                return False

        return True
