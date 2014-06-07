stdoutLog('stdout', true);

function stdoutLog(stream, onload) {
    $.getJSON($SCRIPT_ROOT + '/api/log',
          {
              job_name: jobName,
              task_name: taskName,
              log_id: logId
          },
          function(data) {
              if(typeof(onload)==='undefined') a = false;
              showLogText(stream, data['result'][stream]);
              if (onload == true){
                 $('#header').text($('#header').text() + ' - ' + data['result']['complete_time']);
              }
          }
    );
}


$('#stderr').click(function() {
    stdoutLog('stderr');
});

$('#stdout').click(function() {
    stdoutLog('stdout');
});

function showLogText(logType, value) {
    $('#log-detail').text(value);
    $('#log-detail').scrollTop(0);
    $('#log-detail').removeClass('hidden');
    $('#log-type').text(logType);
}
