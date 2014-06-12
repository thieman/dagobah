""" Basic HTML email template for sending out Dagobah communications. """

from datetime import datetime
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText

import premailer

from .common import EmailTemplate

class BasicEmail(EmailTemplate):

    def send_job_completed(self, data):

        self._format_job_dict(data)
        for task in data.get('tasks', []):
            self._format_task_dict(task)

        template = self._get_template('basic', 'job_completed.html')\
            .render(job=data)
        css = self._get_template('basic', 'job_completed.css').render()

        self.message = self._merge_templates(template, css)
        self._construct_and_send('Job Completed: %s' % data.get('name', None))


    def send_job_failed(self, data):

        self._format_job_dict(data)
        for task in data.get('tasks', []):
            self._format_task_dict(task)

        template = self._get_template('basic', 'job_failed.html')\
            .render(job=data)
        css = self._get_template('basic', 'job_failed.css').render()

        self.message = self._merge_templates(template, css)
        self._construct_and_send('Job Failed: %s' % data.get('name', None))


    def send_task_failed(self, data):

        self._format_task_dict(data)

        template = self._get_template('basic', 'task_failed.html')\
            .render(task=data)
        css = self._get_template('basic', 'task_failed.css').render()

        self.message = self._merge_templates(template, css)
        self._construct_and_send('Task Failed: %s' % data.get('name', None))


    def _format_job_dict(self, job):
        job['next_run'] = self._format_date(job.get('next_run', None))


    def _format_task_dict(self, task):
        success_lu = {None: 'Not executed', True: 'Success',
                      False: 'Failed'}
        task['started_at'] = self._format_date(task.get('started_at', None))
        task['completed_at'] = self._format_date(task.get('completed_at', None))
        task['success'] = success_lu[task.get('success', None)]


    def _merge_templates(self, html, css):
        message = MIMEMultipart()
        content = premailer.transform(''.join(['<html><style>', css, '</style>',
                                               html, '</html>']))
        message.attach(MIMEText(content, 'html'))
        return message


    def _format_date(self, in_date):
        if (not in_date) or (not isinstance(in_date, datetime)):
            return in_date
        return in_date.strftime('%Y-%m-%d %H:%M:%S UTC')
