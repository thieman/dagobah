import logging

from .ses import amazon_ses
from .text import TextEmail
from .basic import BasicEmail


def get_email_handler(handler_name, email_options):
    if isinstance(handler_name, str):
        handler_name = handler_name.lower()

    # Assume authentication is required if missing for backward compatibility.
    auth_required = email_options.get('auth_required', True)

    user = email_options.get('user', None)
    user = user.lower() if isinstance(user, str) else user

    use_amazon = email_options.get('use_amazon', False)
    aws_region_name = email_options.get('region_name', None)
    aws_access_key_id = email_options.get('aws_access_key_id', None)
    aws_secret_access_key = email_options.get('aws_secret_access_key', None)

    if handler_name is None:
        return None

    elif use_amazon:
        if not (aws_region_name and aws_access_key_id and aws_secret_access_key):
            logging.warn("Email.use_amazon is True but required fields are not filled."
                         " Emailing of reports will be disabled.")
            return None
        if handler_name == 'text':
            return amazon_ses(TextEmail)(**email_options)

        elif handler_name == 'basic':
            return amazon_ses(BasicEmail)(**email_options)

    elif auth_required and user is None:
        logging.warn('Email.auth_required is True but Email.user is None. Emailing of reports will be disabled.')
        return None

    elif handler_name == 'text':
        return TextEmail(**email_options)

    elif handler_name == 'basic':
        return BasicEmail(**email_options)

    logging.warn("Dagobahd.email config key unrecognized. Emailing of reports will be disabled.")
    return None
