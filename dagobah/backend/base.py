""" Base Backend class inherited by specific implementations. """

class BaseBackend(object):
    """ Base class for prototypes and compound functions.

    This is also used as the default Backend if the user
    does not specify anything at runtime. In this case,
    calls will proceed normally, but all the methods here
    will do nothing.
    """

    def __init__(self):
        pass


    def __repr__(self):
        return '<BaseBackend>'
