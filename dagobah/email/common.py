""" Common email class to base specific templates off of. """

import smtplib
import socket

class EmailTemplate(object):

    def __init__(self, **kwargs):
        self.formatters = {'{HOSTNAME}': socket.gethostname}
        for kwarg, value in kwargs.iteritems():
            setattr(self, kwarg, value)
        self.from_address = self._apply_formatters(self.from_address)
        self.message = None


    def send_job_complete(self, data):
        raise NotImplementedError()


    def _apply_formatters(self, value):
        new_value = value
        for formatter, call in self.formatters.iteritems():
            new_value = new_value.replace(formatter, call().strip())
        return new_value


    def _address_message(self):
        self.message['From'] = self.from_address
        self.message['To'] = ','.join(self.recipients)


    def _set_subject(self, subject):
        self.message['Subject'] = subject


    def _send_message(self):
        s = smtplib.SMTP('localhost', 1025)
        s.sendmail(self.message['From'],
                   self.message['To'],
                   self.message.as_string())
