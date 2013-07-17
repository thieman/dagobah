## Using Dagobah Directly in Python

Dagobah was primarily designed to be used through the web interface, but it is also possible to replicate all of the core job scheduling and DAG functionality directly in Python. However, some helpful functionality like email alerts will not work out-of-the-box when interfacing with Dagobah directly.

### Quick Start

This example will start a Dagobah instance with a BaseBackend (data is not persisted), create a new Job, create new Tasks in that Job, then schedule the job to run.

```python
from dagobah import Dagobah
from dagobah.backend.base import BaseBackend

# Create a new Dagobah instance, which will manage all of your Jobs
# and the Scheduler instance that will start them.
my_dagobah = Dagobah(BaseBackend())

# Add an empty Job to the Dagobah. This call does not return anything.
my_dagobah.add_job('My Job')

# Grab the new Job and stick it in the my_job variable.
my_job = my_dagobah.get_job('My Job')

# Add new Tasks to the Job with a command to run and an optional name.
# If you don't specify a name, the command is used as the Task's name.
my_job.add_task('python required_task.py', 'Required Task')
my_job.add_task('python dependent_task.py', 'Dependent Task')

# We can also add dependencies between Tasks using their names.
my_job.add_dependency('Required Task', 'Dependent Task')

# Lastly, we schedule the Job to run using Cron syntax.
# Here, we'll tell it to run every morning at 10 AM.
my_job.schedule('0 10 * * *')

# Run indefinitely. Usually Flask.run() does this for us.
from time import sleep
while True:
	  sleep(1)
```

### Using Backends in Python

Backends can be used directly in Python by passing a Backend instance to the Dagobah constructor. Please see the [Backend directory](dagobah/backend/) for details.
