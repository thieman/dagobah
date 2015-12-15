""" Base Backend class inherited by specific implementations. """

import os
import binascii
import json
import logging

from semantic_version import Version

class BaseBackend(object):
    """ Base class for prototypes and compound functions.

    This is also used as the default Backend if the user
    does not specify anything at runtime. In this case,
    calls will proceed normally, but the methods here
    will not persist anything permanently.
    """

    # this is a list of dicts describing the additional packages that
    # need to be installed to use this backend
    # Keys: pypi_name, module_name, version_key, spec_version
    required_packages = []

    def __init__(self):
        self.verify_required_packages()


    def __repr__(self):
        return '<BaseBackend>'


    def verify_required_packages(self):
        failures = []
        for spec in self.required_packages:

            try:
                module = __import__(spec['module_name'])
            except ImportError:
                failures.append('Package {0} not found, please install it. pip install {0}=={1}'.format(spec['pypi_name'],
                                                                                                        spec['version']))
                continue

            installed_version = getattr(module, spec['version_key'])

            if Version(installed_version, partial=True) < Version(spec['version'], partial=True):
                msg = 'Package {0} requires at least version {1}, found version {2}.'.format(spec['pypi_name'],
                                                                                             spec['version'],
                                                                                             installed_version)
                failures.append(msg)
            elif installed_version != spec['version']:
                msg = 'Package {0} has version {1} which is later than specified version {2}.'.format(spec['pypi_name'],
                                                                                                      installed_version,
                                                                                                      spec['version'])
                msg += ' If you experience issues, try downgrading to version {0}.'.format(spec['version'])
                logging.warn(msg)

        if failures:
            for failure in failures:
                logging.error(failure)
            raise ImportError("Package requirements not met for backend {0}.".format(self.__class__.__name__))

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
