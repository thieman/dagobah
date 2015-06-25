import logging

logger = logging.getLogger('dagobah')


class DagobahError(Exception):
    logger.warn('DagobahError being constructed, something must have gone ' +
                'wrong')
    pass
