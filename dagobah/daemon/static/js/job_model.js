function Job() {
	this.loaded = false;
}

Job.prototype.readFromJSON = function(data) {

	this.raw = data;

	this.name = data['name'] || null;
	this.id = data['job_id'] || null;
	this.this_id = data['this_id'] || null;
	this.status = data['status'] || null;
	this.tasks = data['tasks'] || [];
	this.dependencies = data['dependencies'] || {};
	this.cron_schedule = data['cron_schedule'] || null;
	this.next_run = data['next_run'] || null;

}

Job.prototype.load = function(loadJobName, callback) {

	var parent = this;
	parent.loaded = false;

	$.getJSON($SCRIPT_ROOT + '/api/job',
			  {'job_name': loadJobName},
			  function(data) {

				  parent.readFromJSON(data['result']);
				  parent.loaded = true;
				  callback = callback || function() {};
				  callback();

			  }

	);

}

Job.prototype.update = function(callback) {

	if (this.loaded === false) {
		throw "job has not been loaded";
	}

	var parent = this;
	$.getJSON($SCRIPT_ROOT + '/api/job',
			  {'job_name': parent.name},
			  function(data) {

				  parent.readFromJSON(data['result']);
				  parent.loaded = true;
				  callback = callback || function() {};
				  callback();

			  }
	);

}

Job.prototype.forceNode = function(taskName) {
	// map a task name to a force node object
	var task = null;
	for (var i = 0; i < this.tasks.length; i++) {
		if (this.tasks[i].name === taskName) {
			task = this.tasks[i];
		}
	}

	if (task === null) {
		return {};
	}

	var taskStatus = 'waiting';
	if (task.started_at) {
		if (!task.completed_at) {
			taskStatus = 'running';
		} else {
			if (task.success === true) {
				taskStatus = 'complete';
			} else {
				taskStatus = 'failed';
			}
		}
	}

	return {id: taskName, status: taskStatus};
}

Job.prototype.getTaskIndex = function(taskName) {
	// map a task name to its index in Job.tasks
	for (var i = 0; i < this.tasks.length; i++) {
		if (this.tasks[i].name == taskName) {
			return i;
		}
	}
	return -1;
}

Job.prototype.getForceNodes = function() {

	if (!this.loaded) {
		return [];
	}

	var result = [];
	for (var idx = 0; idx < this.tasks.length; idx++) {
		result.push(this.forceNode(this.tasks[idx].name));
	}

	return result;

}

Job.prototype.getForceLinks = function() {

	if (!this.loaded) {
		return [];
	}

	var result = [];
	for (var fromNodeName in this.dependencies) {
		var deps = this.dependencies[fromNodeName];
		for (var idx = 0; idx < deps.length; idx++) {
			result.push({source: this.getTaskIndex(fromNodeName),
						 target: this.getTaskIndex(deps[idx]),
						 left: false, right: true});
		}
	}

	return result;

}
