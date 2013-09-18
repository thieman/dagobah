var hostsTableTemplate = Handlebars.compile($('#hosts-table-template').html());
getHostsData();

function getHostsData() {
	$.getJSON($SCRIPT_ROOT + '/api/hosts', {},
		function(data) {
			renderHostsTable(data['result']);
		}
	);
}

function renderHostsTable(hostsData){
	if (hostsData.length === 0) {
		setTimeout(renderHostsTable, 100);
		return;
	}

	$('#hosts-body').empty();

	for (var i = 0; i < hostsData.length; i++) {
		var thisHost = hostsData[i];
		$('#hosts-body').append(
			hostsTableTemplate({
				hostId: thisHost.host_id,
				hostName: thisHost.host_name,
				hostUsername: thisHost.host_username
			})
		);
	}
}

function deleteHost(hostName, alertId) {

	if (typeof alertId === 'undefined') {
		alertId = 'table-alert';
	}

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/delete_host',
		data: {
			host_name: hostName
		},
		dataType: 'json',
		async: true,
		success: function() {
			showAlert(alertId, 'success', 'Host ' + hostName + ' deleted.');
			$.getJSON($SCRIPT_ROOT + '/api/hosts', {},
				function(data) {
					renderHostsTable(data['result']);
				}
			);
		},
		error: function(e) {
			console.log(e);
			showAlert(alertId, 'error', 'There was an error deleting the task.');
		},
	});
}


$('#hosts-body').on("click", ".host-delete", function() {
	$(this).parents('[data-host]').each(function() {
		deleteHost($(this).attr('data-host'));
	});
});

$('#add-host').click(function() {

	var newHostName = $('#host-name').val();

	if (newHostName === null || newHostName === '') {
		showAlert('new-alert', 'error', 'Please enter a name for the new host.');
		return;
	}

	addNewHost(newHostName);
});

function addNewHost(newHostName) {
	data = {
		host_name: newHostName,
	};

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/add_host',
		data: data,
		dataType: 'json',
		async: true,
		success: function() {
			showAlert('new-alert', 'success', 'Host added.');
			$('#host-name').val('');
			$.getJSON($SCRIPT_ROOT + '/api/hosts', {},
				function(data) {
					renderHostsTable(data['result']);
				}
			);
		},
		error: function() {
			showAlert('new-alert', 'error', 'There was an error adding the host');
		}
	});
}
