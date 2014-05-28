""" Tests on the Mongo backend """

import os
import datetime
import json

import yaml
from nose.tools import nottest
import pymongo

try:
    from pymongo.objectid import ObjectId
except ImportError:
    from bson.objectid import ObjectId

from dagobah.core.core import Dagobah
from dagobah.core.components import StrictJSONEncoder
from dagobah.backend.mongo import MongoBackend


class TestMongo(object):

    @classmethod
    def setup_class(self):
        location = os.path.realpath(os.path.join(os.getcwd(),
                                                 os.path.dirname(__file__)))
        config_file = open(os.path.join(location, 'test_config.yml'))
        config = yaml.load(config_file.read())
        config_file.close()

        if os.getenv('TRAVIS', 'false') == 'true':
            self.mongo_host = '127.0.0.1'
            self.mongo_port = 27017
        else:
            self.mongo_host = config.get('MongoBackend', {}).get('mongo_host')
            self.mongo_port = config.get('MongoBackend', {}).get('mongo_port')

        try:
            try:
                self.client = pymongo.MongoClient(self.mongo_host,
                                                  self.mongo_port)
            except AttributeError:
                self.client = pymongo.Connection(self.mongo_host,
                                                 self.mongo_port)
        except pymongo.errors.ConnectionFailure as e:
            print 'Unable to connect to Mongo at %s:%d' % (self.mongo_host,
                                                           self.mongo_port)
            raise e

        self.db_name = 'dagobah_mongobackend_test'
        if self.db_name in self.client.database_names():
            raise ValueError('test database %s already exists, ' +
                             'please drop it before running the tests'
                             % self.db_name)

        self.db = self.client[self.db_name]
        self.dagobah = None


    @classmethod
    def teardown_class(self):

        # being extra cautious here
        if self.db_name != 'dagobah_mongobackend_test':
            raise ValueError('something modified the test db name, aborting')
        self.client.drop_database(self.db_name)


    @nottest
    def new_dagobah(self, return_instance=False):
        if not return_instance:
            self.dagobah = Dagobah(MongoBackend(self.mongo_host,
                                                self.mongo_port,
                                                self.db_name))
            self.dagobah_coll = self.dagobah.backend.dagobah_coll
            self.job_coll = self.dagobah.backend.job_coll
            self.log_coll = self.dagobah.backend.log_coll
        else:
            return Dagobah(MongoBackend(self.mongo_host,
                                        self.mongo_port,
                                        self.db_name))


    def test_commit_fresh_dagobah(self):
        self.new_dagobah()
        q = {'_id': self.dagobah.dagobah_id,
             'save_date': {'$exists': True},
             'dagobah_id': self.dagobah.dagobah_id,
             'created_jobs': 0,
             'jobs': []}
        assert self.dagobah_coll.find(q).count() == 1


    def test_delete_fresh_dagobah(self):
        self.new_dagobah()
        q = {'_id': self.dagobah.dagobah_id}
        assert self.dagobah_coll.find(q).count() == 1
        self.dagobah.delete()
        assert self.dagobah_coll.find(q).count() == 0


    def test_commit_loaded_dagobah(self):
        self.new_dagobah()
        self.dagobah.add_job('test_job')
        self.dagobah.add_task_to_job('test_job',
                                     'grep dragons',
                                     'do some grepping')

        job = self.dagobah.get_job('test_job')

        q = {'_id': self.dagobah.dagobah_id}
        assert self.dagobah_coll.find(q).count() == 1
        rec = self.dagobah_coll.find_one(q)

        print rec
        assert rec == {'_id': self.dagobah.dagobah_id,
                       'save_date': rec['save_date'],
                       'dagobah_id': self.dagobah.dagobah_id,
                       'created_jobs': 1,
                       'jobs': [{'job_id': job.job_id,
                                 'name': 'test_job',
                                 'parent_id': self.dagobah.dagobah_id,
                                 'tasks': [{'command': 'grep dragons',
                                            'name': 'do some grepping',
                                            'started_at': None,
                                            'completed_at': None,
                                            'success': None,
                                            'soft_timeout': 0,
                                            'hard_timeout': 0,
                                            'hostname': None}],
                                 'dependencies': {'do some grepping': []},
                                 'status': 'waiting',
                                 'cron_schedule': None,
                                 'next_run': None,
                                 'notes': None}]}


    def test_commit_job(self):
        self.new_dagobah()
        self.dagobah.add_job('test_job')
        self.dagobah.add_task_to_job('test_job',
                                     'grep dragons',
                                     'do some grepping')
        job = self.dagobah.get_job('test_job')

        q = {'_id': job.job_id}
        assert self.job_coll.find(q).count() == 1

        rec = self.job_coll.find_one(q)

        print rec
        assert rec == {'_id': job.job_id,
                       'job_id': job.job_id,
                       'name': 'test_job',
                       'parent_id': self.dagobah.dagobah_id,
                       'tasks': [{'name': 'do some grepping',
                                  'command': 'grep dragons',
                                  'completed_at': None,
                                  'started_at': None,
                                  'success': None,
                                  'soft_timeout': 0,
                                  'hard_timeout': 0,
                                  'hostname': None}],
                       'dependencies': {'do some grepping': []},
                       'status': 'waiting',
                       'cron_schedule': None,
                       'next_run': None,
                       'save_date': rec['save_date'],
                       'notes': None}


    def test_construct_from_backend(self):
        self.new_dagobah()
        self.dagobah.add_job('test_job')
        self.dagobah.add_task_to_job('test_job',
                                     'grep dragons',
                                     'do some grepping')
        self.dagobah.add_task_to_job('test_job',
                                     'ls | grep steve',
                                     'find steve')
        job = self.dagobah.get_job('test_job')
        job.add_dependency('do some grepping', 'find steve')

        test_dagobah = self.new_dagobah(return_instance=True)

        test_dagobah.from_backend(self.dagobah.dagobah_id)

        print self.dagobah._serialize()
        print test_dagobah._serialize()

        assert self.dagobah._serialize() == test_dagobah._serialize()


    def test_decode_json(self):
        self.new_dagobah()
        now = datetime.datetime.now()
        test_doc = {"nested": {"dt": now},
                    "object_id": ObjectId('52220d1e6ba8e11a26c20c9a'),
                    "array": [{"object_id": ObjectId('52220d1e6ba8e11a26c20c9b')},
                              {"object_id": ObjectId('52220d1e6ba8e11a26c20c9c'),
                               "string": "woot",
                               "int": 5}]}
        json_doc = json.dumps(test_doc, cls=StrictJSONEncoder)
        result = self.dagobah.backend.decode_import_json(json_doc)
        assert result == test_doc
