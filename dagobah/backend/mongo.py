""" Mongo Backend class built on top of base Backend """

try:
    from pymongo import MongoClient
except ImportError:
    from pymongo import Connection

from dagobah.backend.base import BaseBackend

class MongoBackend(BaseBackend):
    """ Mongo Backend implementation """

    def __init__(self, host, port, db, job_collection='dagobah_job',
                 log_collection='dagobah_log'):
        super(MongoBackend, self).__init__()

        self.host = host
        self.port = port
        self.db_name = db

        try:
            self.client = MongoClient(self.host, self.port)
        except NameError:
            self.client = Connection(self.host, self.port)

        self.db = self.client[self.db_name]

        self.job_coll = self.db[job_collection]
        self.log_coll = self.db[log_collection]


    def __repr__(self):
        return '<MongoBackend (host: %s, port: %s)>' % (self.host, self.port)


    def commit_job(self, job_json):
        append = {'save_date': datetime.utcnow()}
        self.job_coll.save(dict(job_json.items() + append.items()))


    def commit_log(self, log_json):
        append = {'save_date': datetime.utcnow()}
        self.log_coll.save(dict(log_json.items() + append.items()))
