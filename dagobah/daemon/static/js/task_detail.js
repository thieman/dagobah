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
