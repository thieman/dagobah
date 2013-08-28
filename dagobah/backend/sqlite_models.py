""" SQLite model definitions. """

from datetime import datetime
from collections import defaultdict

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

STREAM_LOG_SIZE = 1000000

class Dagobah(Base):
    __tablename__ = 'dagobah'

    id = Column(Integer, primary_key=True)
    created_jobs = Column(Integer, nullable=False)

    jobs = relationship('DagobahJob', backref='parent')

    def __init__(self):
        self.created_jobs = 0

    def __repr__(self):
        return "<SQLite:Dagobah (%d)>" % self.id

    @property
    def json(self):
        return {'dagobah_id': self.id,
                'created_jobs': self.created_jobs,
                'jobs': [job.json for job in self.jobs]}


class DagobahJob(Base):
    __tablename__ = 'dagobah_job'

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('dagobah.id'), index=True)
    name = Column(String(1000))
    status = Column(String(30), nullable=False)
    cron_schedule = Column(String(100))
    next_run = Column(DateTime)

    tasks = relationship('DagobahTask', backref='job')
    dependencies = relationship('DagobahDependency', backref='job')
    logs = relationship('DagobahLog', backref='job')

    def __init__(self, name):
        self.name = name
        self.status = 'waiting'

    def __repr__(self):
        return "<SQLite:DagobahJob (%d)>" % self.id

    @property
    def json(self):
        return {'job_id': self.id,
                'name': self.name,
                'parent_id': self.parent.id,
                'status': self.status,
                'cron_schedule': self.cron_schedule,
                'next_run': self.next_run,
                'tasks': [task.json for task in self.tasks],
                'dependencies': self._gather_dependencies()}

    def update_from_dict(self, data):
        for key in ['parent_id', 'name', 'status', 'cron_schedule',
                    'next_run']:
            if key in data:
                setattr(self, key, data[key])

    def _gather_dependencies(self):
        result = defaultdict(list)
        for dep in self.dependencies:
            result[dep.from_task.name].append(dep.to_task.name)
        return result


class DagobahTask(Base):
    __tablename__ = 'dagobah_task'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('dagobah_job.id'), index=True)
    name = Column(String(1000), nullable=False)
    command = Column(String(1000), nullable=False)
    task_target = Column(String(1000))
    task_target_key = Column(String(1000))
    task_target_password = Column(String(1000))
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    success = Column(String(30))
    soft_timeout = Column(Integer)
    hard_timeout = Column(Integer)

    def __init__(self, name, command):
        self.name = name
        self.command = command
        self.soft_timeout = 0
        self.hard_timeout = 0

    def __repr__(self):
        return "<SQLite:DagobahTask (%d)>" % self.id

    @property
    def json(self):
        return {'name': self.name,
                'command': self.command,
                'started_at': self.started_at,
                'completed_at': self.completed_at,
                'success': self.success,
                'soft_timeout': self.soft_timeout,
                'hard_timeout': self.hard_timeout,
                'task_target': self.task_target,
                'task_target_key': self.task_target_key,
                'task_target_password': self.task_target_password}

    def update_from_dict(self, data):
        for key in ['job_id', 'name', 'command', 'started_at',
                    'completed_at', 'success', 'soft_timeout',
                    'hard_timeout', 'task_target', 'task_target_key', 
                    'task_target_password']:
            if key in data:
                setattr(self, key, data[key])


class DagobahDependency(Base):
    __tablename__ = 'dagobah_dependency'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('dagobah_job.id'), index=True)
    from_task_id = Column(Integer, ForeignKey('dagobah_task.id'), index=True)
    to_task_id = Column(Integer, ForeignKey('dagobah_task.id'), index=True)

    from_task = relationship('DagobahTask', foreign_keys="DagobahDependency.from_task_id")
    to_task = relationship('DagobahTask', foreign_keys="DagobahDependency.to_task_id")

    def __init__(self, from_task_id, to_task_id):
        self.from_task_id = from_task_id
        self.to_task_id = to_task_id

    def __repr__(self):
        return "<SQLite:DagobahDependency (%d)>" % self.id

    def update_from_dict(self, data):
        for key in ['job_id']:
            if key in data:
                setattr(self, key, data[key])


class DagobahLog(Base):
    __tablename__ = 'dagobah_log'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('dagobah_job.id'), index=True)
    start_time = Column(DateTime)
    last_retry_time = Column(DateTime)
    save_date = Column(DateTime)

    tasks = relationship('DagobahLogTask', backref='log')

    def __init__(self):
        self.save_date = datetime.utcnow()

    def __repr__(self):
        return "<SQLite:DagobahLog (%d)>" % self.id

    @property
    def json(self):
        return {'log_id': self.id,
                'job_id': self.job_id,
                'start_time': self.start_time,
                'name': self.job.name,
                'parent_id': self.job.parent_id,
                'tasks': {task.name: task.json
                          for task in self.tasks}}

    def update_from_dict(self, data):
        for key in ['job_id', 'start_time', 'last_retry_time']:
            if key in data:
                setattr(self, key, data[key])
        self.save_date = datetime.utcnow()


class DagobahLogTask(Base):
    __tablename__ = 'dagobah_log_task'

    id = Column(Integer, primary_key=True)
    log_id = Column(Integer, ForeignKey('dagobah_log.id'), index=True)
    name = Column(String(1000), nullable=False)
    start_time = Column(DateTime)
    complete_time = Column(DateTime)
    success = Column(String(30))
    return_code = Column(Integer)
    stdout = Column(String(STREAM_LOG_SIZE))
    stderr = Column(String(STREAM_LOG_SIZE))
    save_date = Column(DateTime)

    def __init__(self, name):
        self.name = name
        self.save_date = datetime.utcnow()

    def __repr__(self):
        return "<SQLite:DagobahLogTask (%d)>" % self.id

    @property
    def json(self):
        return {'success': self.success,
                'return_code': self.return_code,
                'complete_time': self.complete_time,
                'stdout': self.stdout,
                'stderr': self.stderr}

    def update_from_dict(self, data):
        for key in ['name', 'start_time', 'complete_time',
                    'success', 'return_code', 'stdout',
                    'stderr']:
            if key in data:
                setattr(self, key, data[key])
        self.save_date = datetime.utcnow()
