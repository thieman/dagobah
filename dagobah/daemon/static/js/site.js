function showAlert(id, cls, message) {

	alert = $('#' + id);

	$(alert).removeClass();
	$(alert).addClass('alert');
	$(alert).addClass('hidden');
	$(alert).addClass('alert-' + cls);
	$(alert).text(message);
	$(alert).fadeIn(300);

	setTimeout(function() {
		$('#' + id).fadeOut(300, function() {
			$('#' + id).removeClass('alert-' + cls);
		});
	}, 5000);

}

function toTitleCase(str)
{
    return str.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
}
