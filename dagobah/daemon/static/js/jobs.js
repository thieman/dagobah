var jobsData = [];
var updateDataDelayMs = 10000;
updateJobsData();

function updateJobsData() {

	$.getJSON($SCRIPT_ROOT + '/api/jobs',
			  {},
			  function(data) {
				  jobsData = data['result'];
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

				if (transform === 'datetime') {
					if (job[attr] !== null) {
						$(this).text(moment.utc(job[attr]).local().format('lll'));
					}
				} else if (transform === 'class') {
					if (job[attr] !== null) {
						$(this).removeClass().addClass(job[attr]);
					}
				} else if (transform === 'title') {
					if (job[attr] !== null) {
						$(this).text(toTitleCase(job[attr]));
					}
				} else if (transform === 'length') {
					if (job[attr] !== null) {
						$(this).text(job[attr].length);
					}
				}

			}

		});

	});

}

function toTitleCase(str)
{
    return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
}
