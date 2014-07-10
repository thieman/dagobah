import logging

from .text import TextEmail
from .basic import BasicEmail

def get_email_handler(handler_name, email_options):

    if isinstance(handler_name, str):
        handler_name = handler_name.lower()

    #Assume authentication is required if missing for backward compatibility.
    auth_required = email_options.get('auth_required', True)

    user = email_options.get('user', None)
    user = user.lower() if isinstance(user, str) else user

    if handler_name is None:
        return None

    elif auth_required and user is None:
        logging.warn('Email.auth_required is True but Email.user is None. Emailing of reports will be disabled.')
        return None

    elif handler_name == 'text':
        return TextEmail(**email_options)

    elif handler_name == 'basic':
        return BasicEmail(**email_options)

    logging.warn("Dagobahd.email config key unrecognized. Emailing of reports will be disabled.")
    return None
