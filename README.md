Dagobah
=======

Dagobah is a simple dependency-based job scheduler written in Python. Dagobah allows you to schedule periodic jobs using Cron syntax. Each job then kicks off a series of tasks (subprocesses) in an order defined by a dependency graph you can easily draw with click-and-drag in the web interface.

Dagobah lets you retry individual tasks from failure, sends you helpful email reports on job completion and failure, keeps track of your tasks's stdout and stderr, and persists its information in various backends so you don't have to worry about losing your data.

## Installation

    pip install dagobah
    dagobahd  # start the web interface on localhost:9000

On first start, `dagobahd` will create a config file at `~/.dagobahd.yml`. You'll probably want to check that out to get your backend and email options set up before you start using Dagobah.

Dagobah does not require a backend, but unless you specify one, your jobs and tasks will be lost when the daemon exists. Each backend requires its own set of drivers. Once you've installed the drivers, you then need to specify any backend-specific options in the config. See the config file for details.

### Available Backends

To use a backend, you need to install the drivers using the commands and then tell Dagobah to use the backend in the config file (default location `~/.dagobahd.yml`).

#### SQLite

     pip install pysqlite sqlalchemy

#### MongoDB

    pip install pymongo

## Features

## Other Information

#### Known Issues

  * Retrying a failed job after renaming one of its tasks results in an error

#### Planned Features

  * Improved task detail pages
  * Advanced task-level configuration, e.g. timeouts
  * CLI

#### Author

 * [Travis Thieman](https://twitter.com/thieman)

#### Contributors

 * This could be you! [Email](mailto://travis.thieman@gmail.com) or [tweet](https://twitter.com/thieman) me if you have any questions, or create a new issue in this repository.
