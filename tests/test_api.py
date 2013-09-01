""" Tests on API methods. """

import json
import StringIO

from flask import Flask, json
import requests
from nose.tools import nottest

from dagobah.core import Dagobah
from dagobah.daemon.app import app
from dagobah.backend.base import BaseBackend

class TestAPI(object):

    @classmethod
    def setup_class(self):
        self.dagobah = app.config['dagobah']
        self.app = app.test_client()
        self.app.testing = True

        self.app.post('/do-login', data={'password': app.config['APP_PASSWORD']})

        # force BaseBackend and eliminate registered jobs
        # picked up from default backend
        self.dagobah.set_backend(BaseBackend())
        for job in self.dagobah.jobs:
            self.dagobah.delete_job(job.name)

        self.base_url = 'http://localhost:60000'


    @classmethod
    def teardown_class(self):
        self.dagobah.delete()


    @nottest
    def reset_dagobah(self):
        self.dagobah.delete()

        self.dagobah.add_job('Test Job')
        self.dagobah.add_task_to_job('Test Job', 'echo "grep"; sleep 5', 'grep')
        self.dagobah.add_task_to_job('Test Job', 'echo "list"; sleep 5', 'list')
        j = self.dagobah.get_job('Test Job')
        j.add_dependency('grep', 'list')
        j.schedule('0 0 3 0 0')



    @nottest
    def validate_api_call(self, request):
        print request.status_code
        assert request.status_code == 200
        d = json.loads(request.data)
        print d
        assert d['status'] == request.status_code
        assert 'result' in d
        return d


    def test_jobs(self):
        self.reset_dagobah()
        r = self.app.get('/api/jobs')
        d = self.validate_api_call(r)
        assert len(d.get('result', [])) == 1
        assert len(d['result'][0].get('tasks', [])) == 2


    def test_job(self):
        self.reset_dagobah()
        r = self.app.get('/api/job?job_name=Test Job')
        d = self.validate_api_call(r)


    def test_add_and_delete_job(self):
        self.reset_dagobah()
        r = self.app.post('/api/add_job', data={'job_name': 'Test Added Job'})
        d = self.validate_api_call(r)

        r = self.app.get('/api/jobs')
        d = self.validate_api_call(r)
        assert len(d.get('result', [])) == 2

        r = self.app.post('/api/delete_job', data={'job_name': 'Test Added Job'})
        d = self.validate_api_call(r)


    def test_start_job(self):
        self.reset_dagobah()
        r = self.app.post('/api/start_job', data={'job_name': 'Test Job'})
        self.validate_api_call(r)

        r = self.app.get('/api/job?job_name=Test Job')
        d = self.validate_api_call(r)
        assert d['result']['status'] == 'running'


    def test_add_task_to_job(self):
        self.reset_dagobah()
        p_args = {'job_name': 'Test Job',
                  'task_command': 'echo "testing"; sleep 5',
                  'task_name': 'test'}
        r = self.app.post('/api/add_task_to_job', data=p_args)
        self.validate_api_call(r)

        r = self.app.get('/api/jobs')
        d = self.validate_api_call(r)
        assert len(d['result']) == 1
        assert len(d['result'][0]['tasks']) == 3


    def test_add_dependency(self):
        self.reset_dagobah()
        self.dagobah.add_task_to_job('Test Job', 'from node')
        p_args = {'job_name': 'Test Job',
                  'from_task_name': 'from node',
                  'to_task_name': 'grep'}
        r = self.app.post('/api/add_dependency', data=p_args)
        self.validate_api_call(r)

        r = self.app.get('/api/jobs')
        d = self.validate_api_call(r)
        assert len(d['result']) == 1
        assert len(d['result'][0]['tasks']) == 3
        assert d['result'][0]['dependencies']['from node'] == ['grep']


    def test_set_soft_timeout(self):
        self.reset_dagobah()
        p_args = {'job_name': 'Test Job',
                  'task_name': 'grep',
                  'soft_timeout': 30}
        r = self.app.post('/api/set_soft_timeout', data=p_args)
        self.validate_api_call(r)
        assert self.dagobah.get_job('Test Job').tasks['grep'].soft_timeout == 30

        p_args['soft_timeout'] = 0
        r = self.app.post('/api/set_soft_timeout', data=p_args)
        self.validate_api_call(r)
        assert self.dagobah.get_job('Test Job').tasks['grep'].soft_timeout == 0


    def test_set_hard_timeout(self):
        self.reset_dagobah()
        p_args = {'job_name': 'Test Job',
                  'task_name': 'grep',
                  'hard_timeout': 30}
        r = self.app.post('/api/set_hard_timeout', data=p_args)
        self.validate_api_call(r)
        assert self.dagobah.get_job('Test Job').tasks['grep'].hard_timeout == 30

        p_args['hard_timeout'] = 0
        r = self.app.post('/api/set_hard_timeout', data=p_args)
        self.validate_api_call(r)
        assert self.dagobah.get_job('Test Job').tasks['grep'].hard_timeout == 0


    def test_import_export(self):
        self.reset_dagobah()
        req = self.app.get('/api/export_job?job_name=%s' % 'Test Job')
        j = json.loads(req.data)
        self.dagobah.delete()
        assert len(self.dagobah.jobs) == 0

        io = StringIO.StringIO()
        io.write(json.dumps(j))
        io.seek(0)
        r = self.app.post('/api/import_job', data={'file': (io, 'test_upload.json')})
        self.validate_api_call(r)

        assert len(self.dagobah.jobs) == 1
        j = self.dagobah.jobs[0]
        assert j.name == 'Test Job'
        assert len(j.tasks) == 2
