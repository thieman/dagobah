Dagobah
=======

<img src="http://i.imgur.com/D5ZxGXA.png" height="400">

[![Build Status](https://travis-ci.org/thieman/dagobah.png?branch=master)](https://travis-ci.org/thieman/dagobah?branch=master) ![PyPi version](https://pypip.in/v/dagobah/badge.png)

Dagobah is a simple dependency-based job scheduler written in Python. Dagobah allows you to schedule periodic jobs using Cron syntax. Each job then kicks off a series of tasks (subprocesses) in an order defined by a dependency graph you can easily draw with click-and-drag in the web interface.

Dagobah lets you retry individual tasks from failure, sends you helpful email reports on job completion and failure, keeps track of your tasks' stdout and stderr, and persists its information in various backends so you don't have to worry about losing your data.

You can also [use Dagobah directly in Python.](../../wiki/Using Dagobah Directly in Python)

## Installation

Dagobah works with Python 2.6 or 2.7.

    pip install dagobah
    dagobahd  # start the web interface on localhost:9000

On first start, `dagobahd` will create a [config file](dagobah/daemon/dagobahd.yml) at `~/.dagobahd.yml`. You'll probably want to check that out to get your backend and email options set up before you start using Dagobah.

Dagobah does not require a backend, but unless you specify one, your jobs and tasks will be lost when the daemon exits. Each backend requires its own set of drivers. Once you've installed the drivers, you then need to specify any backend-specific options in the config. [See the config file for details.](dagobah/daemon/dagobahd.yml)

### Available Backends

To use a backend, you need to install the drivers using the commands below and then tell Dagobah to use the backend in the config file (default location `~/.dagobahd.yml`).

#### MongoDB

    pip install pymongo
    
#### Deprecated Backends

 * **SQLite**: Deprecated following version 0.3.1.

## Features

#### Single-user auth

<img src="http://i.imgur.com/f843YXK.png" height="200">

#### Manage multiple jobs scheduled with Cron syntax. Run times are shown in your local timezone.

<img src="http://i.imgur.com/PjPQedn.png" height="400">

#### Tasks can be anything you'd normally run at a shell prompt. Pipe and redirect your heart out.

<img src="http://i.imgur.com/mWuQopx.png" height="400">

#### Failed tasks don't break your entire job. Once you fix the task, the job picks up from where it left off.

<img src="http://i.imgur.com/u2vDre2.png" height="400">

#### On completion and failure, Dagobah sends you an email summary of the executed job (just set it up in the config file).

<img src="http://i.imgur.com/yN6LUUZ.png" height="400">

#### Tasks can even be [run on remote machines](https://github.com/thieman/dagobah/wiki/Adding-and-using-remote-hosts-in-Dagobah) (using your SSH config)
<img src="http://i.imgur.com/3sNjJiz.png" height="200">

#### Contributors

 * [Travis Thieman](https://twitter.com/thieman)
 * [Shon T. Urbas](https://github.com/surbas)
 * [Utkarsh Sengar](https://twitter.com/utsengar)
 * Stephanie Wei
 * [Ryan Clough](https://github.com/rclough)

#### Get Started Contributing

 * See the fledgling [wiki](../../wiki) or [create a new issue](../../issues) to get started
 * If you have any questions, go ahead and [email](mailto:travis.thieman@gmail.com) or [tweet at](https://twitter.com/thieman) me, or go ahead and create a new issue in this repository.
