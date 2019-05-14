from __future__ import print_function
import os
from pkg_resources import resource_string

from .core import *


def print_standard_conf():
    """ Print the sample config file to stdout. """
    print(return_standard_conf())


def return_standard_conf():
    """ Return the sample config file. """
    result = resource_string(__name__, 'daemon/dagobahd.yml').decode('ascii')
    result = result % {'app_secret': os.urandom(24).encode('hex')}
    return result
