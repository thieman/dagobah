""" Text email template for sending out Dagobah communications. """

from email.mime.text import MIMEText

from dagobah.email.common import EmailTemplate

class TextEmail(EmailTemplate):

    def send_job_complete(self, data):
        self.message = MIMEText(str(data))
        self._address_message()
        self._set_subject('From Dagobah!')
        self._send_message()
