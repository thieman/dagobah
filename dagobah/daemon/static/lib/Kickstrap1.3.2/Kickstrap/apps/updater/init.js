var updateState; // [0 = up to date, 1 = needs update]
kickstrap.ready(function() {
   getUpdate();
	function getUpdate() { 
         thisVersion = "1.3.0";
			$.ajax({type: 'GET', url: 'http://netdna.getkickstrap.com/update.js', dataType: "script", context: this});
	}
	
	// Qunit test...if turned on.
	if(typeof window.module == 'function' && typeof window.test == 'function') {
	// function exists, so we can now call it
		var _thisVersion = thisVersion; // Cache the current version to put back later.
		setTimeout(function() {
			thisVersion = "super-old-release";
			getUpdate();
		}, 1000);
		setTimeout(function() {
			test("Need for Update", function() {
				equal(updateState, 1, "Successfully recognized need for update");
			});
		}, 2000);
		setTimeout(function() {
			thisVersion = false;
			getUpdate();
		}, 3000);
		setTimeout(function() {
			test("Need for Update", function() {
				equal(updateState, -1, "Successfully recognized failed version retrieval");
				thisVersion = _thisVersion; // Put it back.
			});
		}, 4000);
		
		
	}
});
