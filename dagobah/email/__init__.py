from dagobah.email.text import TextEmail

def get_email_handler(handler_name, email_options):

    if isinstance(handler_name, str):
        handler_name = handler_name.lower()

    if handler_name is None:
        return None

    elif handler_name == 'text':
        return TextEmail(**email_options)
