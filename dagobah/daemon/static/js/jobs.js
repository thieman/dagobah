var jobsTableTemplate = Handlebars.compile($('#jobs-table-template').html());
var jobsData = [];
var updateDataDelayMs = 5000;
updateJobsData();
resetJobsTable();

$('#add-job').click(function() {

	var newName = $('#new-job-name').val();
	if (newName === null || newName === '') {
		showAlert('new-alert', 'error', 'Please enter a name for the new job.');
	}
	addNewJob(newName);

});

function addNewJob(jobName) {

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/add_job',
		data: { job_name: jobName },
		dataType: 'json',
		success: function() {
			showAlert('new-alert', 'success', 'Job created.');
			updateJobsData(true);
		},
		error: function() {
			showAlert('new-alert', 'error', 'There was an error creating the job.');
		},
		async: true
	});

}

function resetJobsTable() {

	if (jobsData.length === 0) {
		setTimeout(resetJobsTable, 100);
		return;
	}

	$('#jobs-body').empty();

	for (var i = 0; i < jobsData.length; i++) {
		var thisJob = jobsData[i];
		$('#jobs-body').append(
			jobsTableTemplate({
				jobName: thisJob.name,
				jobStatus: thisJob.status,
				jobURL: $SCRIPT_ROOT + '/job/' + thisJob.job_id
			})
		);
	}

	updateJobsTable();

}

function updateJobsData(redrawTable) {

	$.getJSON($SCRIPT_ROOT + '/api/jobs',
			  {},
			  function(data) {
				  jobsData = data['result'];
				  if (redrawTable === true) {
					  resetJobsTable();
				  }
				  updateViews();
				  setTimeout(updateJobsData, updateDataDelayMs);
			  }
	);

}

function updateViews() {
	updateJobsTable();
}

function updateJobsTable() {

	$('#jobs-body').children().each(function() {

		var jobName = $(this).attr('data-job');
		for (var i = 0; i < jobsData.length; i++) {
			if (jobsData[i].name === jobName) {
				var job = jobsData[i];
				break;
			}
		}

		$(this).find('[data-attr]').each(function() {

			var attr = $(this).attr('data-attr');
			var transforms = $(this).attr('data-transform') || '';
			transforms = transforms.split(' ');

			$(this).text('');
			if (job[attr] !== null) {
				$(this).text(job[attr]);
			}

			for (var i = 0; i < transforms.length; i++) {
				var transform = transforms[i];
				applyTransformation($(this), job[attr], transform);
			}

		});

	});

}
