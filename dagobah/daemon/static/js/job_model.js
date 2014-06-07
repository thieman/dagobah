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

};

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

};

Job.prototype.update = function(callback) {

    if (this.loaded === false) {
        throw "job has not been loaded";
    }

    // if fallback modal is on screen, don't update
    if ($('#fallback-modal').hasClass('in')) {
        return;
    }

    var parent = this;
    $.ajax({
        type: 'GET',
        url: $SCRIPT_ROOT + '/api/job',
        data: {job_name: parent.name},
        dataType: 'json',
        success: function(data) {
            parent.readFromJSON(data['result']);
            parent.loaded = true;
            callback = callback || function() {};
            callback();
        },
        error: function(data) {
            if ($('#fallback-modal').length === 0) {
                window.location.href = location.origin;
            } else {
                $('#fallback-modal').modal();
            }
        }
    });

};

Job.prototype.forceNode = function(taskName) {
    // map a task name to a force node object
    var task = null;
    for (var i = 0; i < this.tasks.length; i++) {
        if (this.tasks[i].name === taskName) {
            task = this.tasks[i];
        }
    }

    if (task === null) {
        return {id: taskName, status: 'waiting'};
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
};

Job.prototype.getTaskIndex = function(taskName) {
    // map a task name to its index in Job.tasks
    for (var i = 0; i < this.tasks.length; i++) {
        if (this.tasks[i].name == taskName) {
            return i;
        }
    }
    return -1;
};

Job.prototype.getForceNodes = function() {

    if (!this.loaded) {
        return [];
    }

    var result = [];
    for (var idx = 0; idx < this.tasks.length; idx++) {
        result.push(this.forceNode(this.tasks[idx].name));
    }

    return result;

};

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

};

Job.prototype.addDependency = function(link) {
    /// add a new dependency using a link from the graph

    if (!this.loaded) {
        return;
    }

    var that = this;
    var fromName = link.source.id;
    var toName = link.target.id;

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/add_dependency',
        data: {
            job_name: this.name,
            from_task_name: fromName,
            to_task_name: toName
        },
        dataType: 'json',
        async: true,
        success: function() {
            showAlert('graph-alert', 'success', 'Dependency from ' + fromName + ' to ' + toName + ' saved successfully.');
        },
        error: function() {
            showAlert('graph-alert', 'error', "There was an error saving this dependency. The dependency you were trying to add has been removed from the graph.");
            that.removeDependencyFromGraph(fromName, toName);
        }
    });

};

Job.prototype.addTaskToGraph = function(taskName) {
    // add a new node to the graph and refresh it

    if (!this.loaded) {
        return;
    }

    nodes.push(this.forceNode(taskName));
    restartGraph();

};

Job.prototype.removeTaskFromGraph = function(taskName) {
    // remove a node from the graph and refresh it

    if (!this.loaded) {
        return;
    }

    nodes = nodes.filter(function(element) { return element.id !== taskName; });
    links = links.filter(function(element) {
        return (element.source.id !== taskName && element.target.id !== taskName);
    });
    restartGraph();

};

Job.prototype.renameTask = function(oldTaskName, newTaskName) {
    // rename a node in the graph and refresh it

    if (!this.loaded) {
        return;
    }

    for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].id === oldTaskName) {
            nodes[i].id = newTaskName;
        }
    }

    restartGraph();

};

Job.prototype.removeDependencyFromGraph = function(fromTaskName, toTaskName) {

    if (!this.loaded) {
        return;
    }

    links = links.filter(function(element) {
        return (element.source.id !== fromTaskName || element.target.id !== toTaskName);
    });
    restartGraph();

};
