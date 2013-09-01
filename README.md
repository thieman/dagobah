Dagobah
=======

<img src="http://i.imgur.com/D5ZxGXA.png" height="400">

[![Build Status](https://travis-ci.org/tthieman/dagobah.png?branch=master)](https://travis-ci.org/tthieman/dagobah?branch=master) ![PyPi version](https://pypip.in/v/dagobah/badge.png)

Dagobah is a simple dependency-based job scheduler written in Python. Dagobah allows you to schedule periodic jobs using Cron syntax. Each job then kicks off a series of tasks (subprocesses) in an order defined by a dependency graph you can easily draw with click-and-drag in the web interface.

Dagobah lets you retry individual tasks from failure, sends you helpful email reports on job completion and failure, keeps track of your tasks's stdout and stderr, and persists its information in various backends so you don't have to worry about losing your data.

You can also [use Dagobah directly in Python.](docs/python_example.md)

## Installation

    pip install dagobah
    dagobahd  # start the web interface on localhost:9000

On first start, `dagobahd` will create a [config file](dagobah/daemon/dagobahd.yml) at `~/.dagobahd.yml`. You'll probably want to check that out to get your backend and email options set up before you start using Dagobah.

Dagobah does not require a backend, but unless you specify one, your jobs and tasks will be lost when the daemon exits. Each backend requires its own set of drivers. Once you've installed the drivers, you then need to specify any backend-specific options in the config. [See the config file for details.](dagobah/daemon/dagobahd.yml)

### Available Backends

To use a backend, you need to install the drivers using the commands below and then tell Dagobah to use the backend in the config file (default location `~/.dagobahd.yml`).

#### SQLite

     pip install pysqlite sqlalchemy

#### MongoDB

    pip install pymongo

## Features

#### Single-user auth (new in 0.1.2)

<img src="http://i.imgur.com/f843YXK.png" height="200">

#### Manage multiple jobs scheduled with Cron syntax. Run times are shown in your local timezone.

<img src="http://i.imgur.com/PjPQedn.png" height="400">

#### Tasks can be anything you'd normally run at a shell prompt. Pipe and redirect your heart out.

<img src="http://i.imgur.com/mWuQopx.png" height="400">

#### Failed tasks don't break your entire job. Once you fix the task, the job picks up from where it left off.

<img src="http://i.imgur.com/u2vDre2.png" height="400">

#### On completion and failure, Dagobah sends you an email summary of the executed job (just set it up in the config file).

<img src="http://i.imgur.com/yN6LUUZ.png" height="400">

#### Author

 * [Travis Thieman](https://twitter.com/thieman)

#### Contributors

 * This could be you! If you have any questions, go ahead and [email](mailto:travis.thieman@gmail.com) or [tweet at](https://twitter.com/thieman) me, or go ahead and create a new issue in this repository.
