from dagobah.email.text import TextEmail
from dagobah.email.basic import BasicEmail

def get_email_handler(handler_name, email_options):

    if isinstance(handler_name, str):
        handler_name = handler_name.lower()

    user = email_options.get('user', None)
    user = user.lower() if isinstance(user, str) else user

    if (handler_name is None or user is None or user == 'none'):
        return None

    elif handler_name == 'text':
        return TextEmail(**email_options)

    elif handler_name == 'basic':
        return BasicEmail(**email_options)
