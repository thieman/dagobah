""" Text email template for sending out Dagobah communications. """

from datetime import datetime
from email.mime.text import MIMEText

from .common import EmailTemplate

class TextEmail(EmailTemplate):

    def send_job_completed(self, data):
        self.message = MIMEText(self._job_to_text(data))
        self._construct_and_send('Job Completed: %s' % data.get('name', None))


    def send_job_failed(self, data):
        self.message = MIMEText(self._job_to_text(data))
        self._construct_and_send('Job Failed: %s' % data.get('name', None))


    def send_task_failed(self, data):
        self.message = MIMEText(self._task_to_text(data))
        self._construct_and_send('Task Failed: %s' % data.get('name', None))


    def _task_to_text(self, task):
        """ Return a standard formatting of a Task serialization. """

        started = self._format_date(task.get('started_at', None))
        completed = self._format_date(task.get('completed_at', None))

        success = task.get('success', None)
        success_lu = {None: 'Not executed', True: 'Success',
                      False: 'Failed'}

        run_log = task.get('run_log', {})
        return '\n'.join(['Task: %s' % task.get('name', None),
                          'Command: %s' % task.get('command', None),
                          'Result: %s' % success_lu[success],
                          'Started at: %s' % started,
                          'Completed at: %s' % completed,
                          'Return Code: %s' % run_log.get('return_code', None),
                          'Stdout: %s' % run_log.get('stdout', None),
                          'Stderr: %s' % run_log.get('stderr', None)])


    def _job_to_text(self, job):
        """ Return a standard formatting of a Job serialization. """

        next_run = self._format_date(job.get('next_run', None))

        tasks = ''
        for task in job.get('tasks', []):
            tasks += self._task_to_text(task)
            tasks += '\n\n'

        return '\n'.join(['Job name: %s' % job.get('name', None),
                          'Cron schedule: %s' % job.get('cron_schedule', None),
                          'Next run: %s' % next_run,
                          '',
                          'Parent ID: %s' % job.get('parent_id', None),
                          'Job ID: %s' % job.get('job_id', None),
                          '',
                          'Tasks Detail',
                          '',
                          tasks])


    def _format_date(self, in_date):
        if (not in_date) or (not isinstance(in_date, datetime)):
            return in_date
        return in_date.strftime('%Y-%m-%d %H:%M:%S UTC')
