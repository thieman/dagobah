import os

from .core import *

config_file_default_location = os.path.realpath(os.path.join(os.getcwd(),
                                                             'dagobah/daemon'))

def print_standard_conf():
    """ Print the sample config file to stdout. """
    print return_standard_conf()

def return_standard_conf():
    """ Return the sample config file. """
    config_file = open(os.path.join(config_file_default_location, 'dagobahd.yml'))
    result = config_file.read()
    config_file.close()
    result = result % {'app_secret': os.urandom(24).encode('hex')}
    return result
