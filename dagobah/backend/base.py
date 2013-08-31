""" Base Backend class inherited by specific implementations. """

import os
import binascii
import json

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


    def decode_import_json(self, json_doc, transformers=None):
        """ Decode a JSON string based on a list of transformers.

        Each transformer is a pair of ([conditional], transformer). If
        all conditionals are met on each non-list, non-dict object,
        the transformer tries to apply itself.

        conditional: Callable that returns a Bool.
        transformer: Callable transformer on non-dict, non-list objects.
        """

        def custom_decoder(dct):

            def transform(o):

                if not transformers:
                    return o

                for conditionals, transformer in transformers:

                    conditions_met = True
                    for conditional in conditionals:
                        try:
                            condition_met = conditional(o)
                        except:
                            condition_met = False
                        if not condition_met:
                            conditions_met = False
                            break

                    if not conditions_met:
                        continue

                    try:
                        return transformer(o)
                    except:
                        pass

                return o

            for key in dct.iterkeys():
                if isinstance(key, dict):
                    custom_decoder(dct[key])
                elif isinstance(key, list):
                    [custom_decoder[elem] for elem in dct[key]]
                else:
                    dct[key] = transform(dct[key])

            return dct

        return json.loads(json_doc, object_hook=custom_decoder)


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
