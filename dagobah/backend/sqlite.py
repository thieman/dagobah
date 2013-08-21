""" SQLite Backend class built on top of base Backend """

import os
from datetime import datetime
from copy import deepcopy
import threading

import sqlalchemy

from dagobah.backend.base import BaseBackend
from dagobah.backend.sqlite_models import (Base, Dagobah, DagobahJob,
                                           DagobahTask, DagobahDependency,
                                           DagobahLog, DagobahLogTask)


class SQLiteBackend(BaseBackend):
    """ SQLite Backend implementation. """

    def __init__(self, filepath):
        super(SQLiteBackend, self).__init__()

        self.filepath = filepath
        if self.filepath == 'default':
            location = os.path.realpath(os.path.join(os.getcwd(),
                                                     os.path.dirname(__file__)))
            self.filepath = os.path.join(location, 'dagobah.db')

        connect_args = {'check_same_thread': False}
        self.engine = sqlalchemy.create_engine('sqlite:///' + self.filepath,
                                               connect_args=connect_args)
        self.Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = self.Session()

        Base.metadata.create_all(self.engine)

        self.lock = threading.Lock()


    def __repr__(self):
        return '<SQLiteBackend (path: %s)>' % (self.filepath)


    def get_known_dagobah_ids(self):
        results = []
        for rec in self.session.query(Dagobah).all():
            results.append(rec.id)
        return results


    def get_new_dagobah_id(self):
        count = self.session.query(sqlalchemy.func.max(Dagobah.id)).scalar()
        return max(count, 0) + 1


    def get_new_job_id(self):
        count = self.session.query(sqlalchemy.func.max(DagobahJob.id)).scalar()
        return max(count, 0) + 1


    def get_new_log_id(self):
        count = self.session.query(sqlalchemy.func.max(DagobahLog.id)).scalar()
        return max(count, 0) + 1


    def get_dagobah_json(self, dagobah_id):
        return self.session.query(Dagobah).\
            filter_by(id=dagobah_id).\
            one().json


    def commit_dagobah(self, dagobah_json):
        rec = self.session.query(Dagobah).\
            filter_by(id=dagobah_json['dagobah_id']).\
            first()

        if not rec:
            rec = Dagobah()
            self.session.add(rec)

        for job in dagobah_json.get('jobs', []):

            existing = self.session.query(DagobahJob).\
                filter_by(id=job['job_id']).\
                first()

            if existing:
                self._update_job_rec(existing, dagobah_json, 'dagobah')
            else:
                new_job = DagobahJob(job['name'])
                rec.jobs.append(new_job)
                self._update_job_rec(new_job, dagobah_json, 'dagobah')

        self.session.commit()


    def delete_dagobah(self, dagobah_id):
        """ Deletes the Dagobah and all child Jobs from the database.

        Related run logs are deleted as well.
        """

        rec = self.session.query(Dagobah).filter_by(id=dagobah_id).first()
        if not rec:
            raise KeyError('no dagobah doc found with id %s' % dagobah_id)

        for job in rec.jobs:
            self.delete_job(job.id)

        self.session.delete(rec)
        self.session.commit()


    def commit_job(self, job_json):

        rec = self.session.query(DagobahJob).\
            filter_by(id=job_json['job_id']).\
            first()

        if not rec:
            rec = DagobahJob(job_json['name'])
            self.session.add(rec)

        self._update_job_rec(rec, job_json, 'job')
        self.session.commit()


    def delete_job(self, job_id):

        # TODO: get cascading deletes to work automatically with sqlite
        # this should be a generalizable solution for all supported DBs, though

        job = self.session.query(DagobahJob).filter_by(id=job_id).one()

        for task in job.tasks:
            self.session.delete(task)
        for dep in job.dependencies:
            self.session.delete(dep)
        for log in job.logs:
            for task in log.tasks:
                self.session.delete(task)
            self.session.delete(log)
        self.session.delete(job)

        self.session.commit()


    def commit_log(self, log_json):

        rec = self.session.query(DagobahLog).\
            filter_by(id=log_json['log_id']).\
            first()

        if rec:
            rec.update_from_dict(log_json)
        else:
            rec = DagobahLog()
            self.session.add(rec)

        # delete any deprecated logtask records for this log
        for log_task in self.session.query(DagobahLogTask).\
            filter_by(log_id=rec.id).\
            all():
            if log_task.name not in log_json.get('tasks', {}).keys():
                self.session.delete(log_task)

        self.session.flush()

        for task_name, task_data in log_json.get('tasks', {}).iteritems():

            existing = self.session.query(DagobahLogTask).\
                filter_by(log_id=rec.id).\
                filter_by(name=task_name).\
                first()

            if not existing:
                existing = DagobahLogTask(task_name)
                self.session.add(existing)
                rec.tasks.append(existing)

            existing.update_from_dict(task_data)

        self.session.commit()


    def get_latest_run_log(self, job_id, task_name):
        log = self.session.query(DagobahLog).\
            filter_by(job_id=job_id).\
            order_by(DagobahLog.save_date.desc()).\
            first()
        return log.json


    def acquire_lock(self):
        self.lock.acquire()


    def release_lock(self):
        self.lock.release()


    def _update_job_rec(self, job_rec, in_data, data_type):
        """" Update the passed DagobahJob record and its children Tasks.

        Does not commit the session after its updates.
        """

        if data_type not in ['dagobah', 'job']:
            raise KeyError('unknown data_type %s' % data_type)

        data = deepcopy(in_data)

        if data_type == 'dagobah':

            found_job = False
            for job in data.get('jobs', []):
                if job['job_id'] == job_rec.id:
                    data = job
                    found_job = True
                    break

            if not found_job:
                raise KeyError('job %s not found in in_data' % job_rec.name)

        # update the job record itself
        job_rec.update_from_dict(data)

        job_tasks = data.get('tasks', {})
        job_deps = data.get('dependencies', {})

        # clean up tasks and dependencies that have been removed
        for existing_task in self.session.query(DagobahTask).\
                filter_by(job_id=job_rec.id).\
                all():
            if existing_task.name not in job_deps:
                self.session.query(DagobahDependency).\
                    filter(sqlalchemy.or_(DagobahDependency.from_task_id==existing_task.id,
                                          DagobahDependency.to_task_id==existing_task.id)).\
                    delete()
                self.session.delete(existing_task)

        for existing_dep in self.session.query(DagobahDependency).\
                filter_by(job_id=job_rec.id).\
                all():
            if (existing_dep.from_task.name not in job_deps or
                (existing_dep.to_task.name not in
                 job_deps[existing_dep.from_task.name])):
                self.session.delete(existing_dep)

        # update and create tasks
        for task_data in job_tasks:
            existing = self.session.query(DagobahTask).\
                filter_by(job_id=job_rec.id).\
                filter_by(name=task_data['name']).\
                first()

            if not existing:
                existing = DagobahTask(task_data['name'], task_data['command'])
                self.session.add(existing)
                job_rec.tasks.append(existing)

            self._update_task_rec(existing, data)

        # get up-to-date task IDs and create a name-to-ID lookup
        self.session.flush()
        task_lu = {}
        for task in job_rec.tasks:
            task_lu[task.name] = task.id

        # update and create dependencies
        for from_task_name, to_tasks in job_deps.iteritems():
            for to_task_name in to_tasks:
                from_task_id = task_lu[from_task_name]
                to_task_id = task_lu[to_task_name]
                existing = self.session.query(DagobahDependency).\
                    filter_by(job_id=job_rec.id).\
                    filter_by(from_task_id=from_task_id).\
                    filter_by(to_task_id=to_task_id).\
                    first()

                if not existing:
                    existing = DagobahDependency(from_task_id, to_task_id)
                    self.session.add(existing)
                    job_rec.dependencies.append(existing)


    def _update_task_rec(self, task_rec, job_data):
        """ Update the passed DagobahTask from a job data dict. """
        for task in job_data.get('tasks', []):
            if task.get('name', None) == task_rec.name:
                task_rec.update_from_dict(task)
