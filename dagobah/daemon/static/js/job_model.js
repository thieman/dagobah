function Job() {
	this.loaded = false;
}

Job.prototype.load = function(loadJobName) {

	var parent = this;
	$.getJSON($SCRIPT_ROOT + '/api/job',
			  {'job_name': loadJobName},
			  function(data) {

				  var result = data['result'];
				  parent.raw = result;

				  parent.name = result['name'] || null;
				  parent.id = result['job_id'] || null;
				  parent.parent_id = result['parent_id'] || null;
				  parent.tasks = result['tasks'] || [];
				  parent.dependencies = result['dependencies'] || {};
				  parent.cron_schedule = result['cron_schedule'] || null;
				  parent.next_run = result['next_run'] || null;

				  parent.loaded = true;

			  });

}

Job.prototype.forceNode = function(taskName) {
	// map a task name to a force node object
	return {id: taskName};
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
