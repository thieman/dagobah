""" Base Backend class inherited by specific implementations. """

import os
import binascii

class BaseBackend(object):
    """ Base class for prototypes and compound functions.

    This is also used as the default Backend if the user
    does not specify anything at runtime. In this case,
    calls will proceed normally, but the methods here
    will not persist anything permanently.
    """

    def __init__(self):
        pass


    def __repr__(self):
        return '<BaseBackend>'


    def get_known_dagobah_ids(self):
        return []


    def get_new_dagobah_id(self):
        return binascii.hexlify(os.urandom(16))


    def get_new_job_id(self):
        return binascii.hexlify(os.urandom(16))


    def get_new_log_id(self):
        return binascii.hexlify(os.urandom(16))


    def get_dagobah_json(self, dagobah_id):
        return


    def commit_dagobah(self, dagobah_json):
        return


    def delete_dagobah(self, dagobah_id):
        return


    def commit_job(self, job_json):
        pass


    def delete_job(self, job_name):
        pass


    def commit_log(self, log_json):
        pass


    def get_latest_run_log(self, job_id, task_name):
        return {}


    def acquire_lock(self):
        return


    def release_lock(self):
        return
