### v0.1.3 ()

  * Add soft (SIGTERM) and hard (SIGKILL) timeouts to Tasks

### v0.1.2 (June 26, 2013)

 * Add single-user auth mechanism
 * Added Dagobahd.app_secret and Dagobahd.password config keys
 * Missing config keys will now be replaced by defaults and place a warning in the log file and stdout
 * Failed tasks can now be restarted before all running tasks complete

### v0.1.1 (May 31, 2013)

 * Fix broken install due to incorrect reference to standard log file
 * Added missing requirements for install and for running tests
