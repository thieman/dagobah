function runWhenJobLoaded() {
	if (typeof job != 'undefined' && job.loaded === true) {
		setControlButtonStates();
	} else {
		setTimeout(runWhenJobLoaded, 50);
	}
}

runWhenJobLoaded();

function setControlButtonStates() {
	// disable control buttons based on current job state

	$('#start-job').prop('disabled', false);
	$('#retry-job').prop('disabled', false);
	$('#terminate-job').prop('disabled', false);
	$('#kill-job').prop('disabled', false);

	if (job.status == 'waiting') {
		$('#terminate-job').prop('disabled', true);
		$('#kill-job').prop('disabled', true);
	} else if (job.status == 'running') {
		$('#start-job').prop('disabled', true);
		$('#retry-job').prop('disabled', true);
	} else if (job.status == 'failed') {
		$('#terminate-job').prop('disabled', true);
		$('#kill-job').prop('disabled', true);
	}

}

$('#start-job').click(function() {

	if (!job.loaded) {
		return;
	}

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/start_job',
		data: {job_name: job.name},
		dataType: 'json',
		success: function() {
			showAlert('state-alert', 'success', 'Job started');
		},
		error: function() {
			showAlert('state-alert', 'error', 'Unable to start job');
		},
		async: true
	});

});

$('#retry-job').click(function() {

	if (!job.loaded) {
		return;
	}

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/retry_job',
		data: {job_name: job.name},
		dataType: 'json',
		success: function() {
			showAlert('state-alert', 'success', 'Retrying job from previous state');
		},
		error: function() {
			showAlert('state-alert', 'error', 'Unable to retry job');
		},
		async: true
	});

});

$('#terminate-job').click(function() {
});

$('#kill-job').click(function() {
});

function showAlert(id, cls, message) {

	alert = $('#' + id);
	$(alert).addClass('alert-' + cls);
	$(alert).text(message);
	$(alert).fadeIn(300);
	setTimeout(function() {
		$('#' + id).fadeOut(300);
		$('#' + id).removeClass('alert-' + cls);
	}, 5000);

}
