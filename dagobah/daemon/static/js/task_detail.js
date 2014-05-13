var historyTableTemplate = Handlebars.compile($('#history-table-template').html());
var historyNameTemplate = Handlebars.compile($('#history-name-template').html());

Handlebars.registerPartial('historyId',historyNameTemplate);

var historyData = [];
loadHistoryTable();

$('#save-soft-timeout').click(function() {

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/set_soft_timeout',
		data: {
			job_name: jobName,
			task_name: taskName,
			soft_timeout: $('#soft-timeout').val()
		},
		dataType: 'json',
		async: true,
		success: function() {
			showAlert('soft-timeout-alert', 'success', 'Soft timeout set');
		},
		error: function() {
			showAlert('soft-timeout-alert', 'error', 'There was an error setting the soft timeout');
		}
	});

});

$('#save-hard-timeout').click(function() {

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/set_hard_timeout',
		data: {
			job_name: jobName,
			task_name: taskName,
			hard_timeout: $('#hard-timeout').val()
		},
		dataType: 'json',
		async: true,
		success: function() {
			showAlert('hard-timeout-alert', 'success', 'Hard timeout set');
		},
		error: function() {
			showAlert('hard-timeout-alert', 'error', 'There was an error setting the hard timeout');
		}
	});

});

function showLogText(logType, value) {
	var newText = '';
	for (var i = 0; i < value.length; i++) {
		newText += value[i] + '\n';
	}
	$('#log-detail').text(newText);
	$('#log-detail').scrollTop(0);
	$('#log-detail').removeClass('hidden');
	$('#log-type').text(logType);
}

$('#head-stdout').click(function() {

	$.getJSON($SCRIPT_ROOT + '/api/head',
			  {
				  job_name: jobName,
				  task_name: taskName,
				  stream: 'stdout',
				  num_lines: 100
			  },
			  function(data) {
				  showLogText('Head: Stdout', data['result']);
			  }
	);

});


$('#tail-stdout').click(function() {

	$.getJSON($SCRIPT_ROOT + '/api/tail',
			  {
				  job_name: jobName,
				  task_name: taskName,
				  stream: 'stdout',
				  num_lines: 100
			  },
			  function(data) {
				  showLogText('Tail: Stdout', data['result']);
			  }
	);

});


$('#head-stderr').click(function() {

	$.getJSON($SCRIPT_ROOT + '/api/head',
			  {
				  job_name: jobName,
				  task_name: taskName,
				  stream: 'stderr',
				  num_lines: 100
			  },
			  function(data) {
				  showLogText('Head: Stderr', data['result']);
			  }
	);

});


$('#tail-stderr').click(function() {

	$.getJSON($SCRIPT_ROOT + '/api/tail',
			  {
				  job_name: jobName,
				  task_name: taskName,
				  stream: 'stderr',
				  num_lines: 100
			  },
			  function(data) {
				  showLogText('Tail: Stderr', data['result']);
			  }
	);

});

function loadHistoryTable() {

	$.getJSON($SCRIPT_ROOT + '/api/logs',
			  {
				  job_name: jobName,
				  task_name: taskName,
			  },
			  function(data) {
				  data = data.result;
				  renderHistoryTable(data);
			  }
			 );
}

function convertToIsoTime(timestring) {
	var string_split = timestring.split(" ");
	var isoTime = string_split[0] + "T" + string_split[1];
	return isoTime;
}

function determineRuntime(completion, start){
	// Backwards compatability: start time was not stored for logs before
	if (start === undefined) {
		return "Runtime not available."
	}
	var completion_string = convertToIsoTime(completion);
	var completion_time = new Date(completion_string).getTime();
	var start_string = convertToIsoTime(start);
	var starting_time = new Date(start_string).getTime();
	var milliseconds = completion_time - starting_time;
	var numhours = Math.floor(milliseconds / 3600000);
	milliseconds = milliseconds - (numhours * 3600000);
	var numminutes = Math.floor(milliseconds / 60000);
	milliseconds = milliseconds - (numminutes * 60000);
	var numseconds = Math.floor(milliseconds / 1000);
	milliseconds = milliseconds - (numseconds * 1000);
	var result = String(numhours) + ":" + String(numminutes) + ":" + String(numseconds) + "." + String(milliseconds);
	return result;
}

function renderHistoryTable(data){
	if (data.length === 0) {
		setTimeout(renderHistoryTable, 100);
		return;
	}

	$('#history-body').empty();

	for (var i = 0; i < data.length; i++) {
		var thisJob = data[i];
		var start_time = thisJob['tasks'][taskName].start_time;
		var completion_time = thisJob['tasks'][taskName].complete_time;
		var run_time = null;
		if (completion_time === undefined) {
			run_time = 'Did not complete.';
		} else {
			run_time = determineRuntime(completion_time, start_time);
		}

		$('#history-body').append(
			historyTableTemplate({
			historyId: thisJob.log_id,
			runtime: run_time,
			completionTime: completion_time,
			logURL: $SCRIPT_ROOT + '/job/' + thisJob.job_id + '/' + taskName + '/' + thisJob.log_id
		})
		);
	}
}
