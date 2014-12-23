""" Common email class to base specific templates off of. """

import os
import smtplib
import socket
import email.utils

import jinja2

class EmailTemplate(object):

    def __init__(self, **kwargs):

        self.location = os.path.realpath(os.path.join(os.getcwd(),
                                                      os.path.dirname(__file__)))

        self.formatters = {'{HOSTNAME}': socket.gethostname}
        for kwarg, value in kwargs.iteritems():
            setattr(self, kwarg, value)
        self.from_address = self._apply_formatters(self.from_address)
        self.message = None


    def send_job_completed(self, data):
        raise NotImplementedError()


    def send_task_failed(self, data):
        raise NotImplementedError()


    def send_job_failed(self, data):
        raise NotImplementedError()


    def _construct_and_send(self, subject):
        self._address_message()
        self._set_subject(subject)
        self._send_message()


    def _apply_formatters(self, value):
        new_value = value
        for formatter, call in self.formatters.iteritems():
            new_value = new_value.replace(formatter, call().strip())
        return new_value


    def _address_message(self):
        email_addr = self.from_address if self.user is None else self.user

        self.message['From'] = email.utils.formataddr((self.from_address, email_addr))
        self.message['To'] = ','.join(self.recipients)


    def _set_subject(self, subject):
        self.message['Subject'] = subject


    def _send_message(self):
        s = smtplib.SMTP(self.host, self.port)
        if self.use_tls:
            s.ehlo()
            s.starttls()
            s.ehlo

        if getattr(self, 'auth_required', True):  #Preserve backward compatibility
            s.login(self.user, self.password)

        s.sendmail(self.message['From'],
                   self.recipients,
                   self.message.as_string())


    def _get_template(self, template_name, template_file):
        """ Returns a Jinja2 template of the specified file. """
        template = os.path.join(self.location, 'templates',
                                template_name, template_file)
        return jinja2.Template(open(template).read())
