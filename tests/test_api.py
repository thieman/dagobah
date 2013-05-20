""" Tests on API methods. """

from flask import Flask
import requests

from dagobah.daemon import daemon

class TestAPI(object):

    @classmethod
    def setup_class(self):
        self.dagobah = daemon.dagobah
        self.app = daemon.app.test_client()

        self.base_url = 'http://localhost:9000'


    @classmethod
    def teardown_class(self):
        pass


    def test_jobs(self):
        assert 'Dashboard' in self.app.get('/', follow_redirects=True).data
