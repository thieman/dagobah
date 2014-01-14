### v0.2.2 (January 7, 2014)

  * Bug fix

### v0.2.1 (January 2, 2014)

  * Added compatibility with Python 2.6

### v0.2.0 (September 6, 2013)

  * Added soft (SIGTERM) and hard (SIGKILL) timeouts to Tasks
  * SQLite database migrations are now automatically handled by Alembic
  * Added job-level JSON import and export
  * Added Dagobahd.auth_disabled config key
  * Single-user auth can now be disabled through the app config
  * Updated Flask-Login to 0.2.6

### v0.1.2 (June 26, 2013)

 * Added single-user auth mechanism
 * Added Dagobahd.app_secret and Dagobahd.password config keys
 * Missing config keys will now be replaced by defaults and place a warning in the log file and stdout
 * Failed tasks can now be restarted before all running tasks complete

### v0.1.1 (May 31, 2013)

 * Fixed broken install due to incorrect reference to standard log file
 * Added missing requirements for install and for running tests
