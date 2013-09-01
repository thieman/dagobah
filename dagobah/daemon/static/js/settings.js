$('#add-host').click(function() {

	var newHostName = $('#host-name').val();
	var newHostUsername = $('#host-username').val();
	var newHostKey= $('#host-key').val();
	var newHostPassword = $('#host-password').val();

	if (newHostName === null || newHostName === '') {
		showAlert('new-alert', 'error', 'Please enter a name for the new host.');
		return;
	}
	if (newHostUsername === null || newHostUsername === '') {
		showAlert('new-alert', 'error', 'Please enter a username for the new host.');
		return;
	}

	if ((newHostKey === null || newHostKey === '') && (newHostPassword === null || newHostPassword === '')) {
		showAlert('new-alert', 'error', 'Please enter ssh key or password for the new host.');
		return;
	}

	if (newHostKey !== '' && newHostPassword !== '') {
		showAlert('new-alert', 'error', 'Please enter either ssh key or password for the new host. Not both.');
		return;
	}

	addNewHost(newHostName, newHostUsername, newHostKey, newHostPassword);

});

function addNewHost(newHostName, newHostUsername, newHostKey, newHostPassword) {
	if (newHostKey !== ''){
		data = {
			host_name: newHostName,
			host_username: newHostUsername,
			host_key: newHostKey
		};
	} else if (newHostPassword !== ''){
		data = {
			host_name: newHostName,
			host_username: newHostUsername,
			host_password: newHostPassword
		};
	}

	$.ajax({
		type: 'POST',
		url: $SCRIPT_ROOT + '/api/add_host',
		data: data,
		dataType: 'json',
		success: function() {
			showAlert('new-alert', 'success', 'Host added.');
			$('#host-name').val('');
			$('#host-username').val('');
			$('#host-key').val('');
			$('#host-password').val('');
		},
		error: function() {
			showAlert('new-alert', 'error', 'There was an error adding the host');
		},
		async: true
	});
}
