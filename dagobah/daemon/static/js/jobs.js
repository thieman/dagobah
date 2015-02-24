var jobsTableTemplate = Handlebars.compile($('#jobs-table-template').html());
var jobNameTemplate = Handlebars.compile($('#job-name-template').html());
var editJobNameTemplate = Handlebars.compile($('#edit-job-name-template').html());

Handlebars.registerPartial('jobName', jobNameTemplate);

var jobsData = [];
var updateDataDelayMs = 5000;
var jobsDataTimeout = null;
updateJobsData();
resetJobsTable();

$('#add-job').click(function() {

    var newName = $('#new-job-name').val();
    if (newName === null || newName === '') {
        showAlert('new-alert', 'error', 'Please enter a name for the new job.');
    }
    $('#new-job-name').val('');
    addNewJob(newName);

});

$('#import-job').click(function() {
});

function onJobDeleteClick() {
    var that = this;
    var modalOpts = [{
        "label": "Cancel"
    }, {
        "label": "Delete",
        "class": "btn-danger",
        "callback": function() {
            $(that).parents('[data-job]').each(function() {
                deleteJob($(this).attr('data-job'));
            });
        }
    }];
    bootbox.dialog("Are you sure you want to delete this job?", modalOpts);
}

function onEditJobClick() {
    var td = $(this).parent();
    var tr = $(td).parent();

    td.remove();
    tr.prepend(editJobNameTemplate({ jobName: $(tr).attr('data-job') }));
    $(tr).find('>:first-child').find('input').select();
    bindEvents();
}

function onSaveJobClick() {

    var input = $(this).siblings('input');
    var jobName = $(input).attr('data-original-name');
    var newName = $(input).val();

    if (newName !== null && newName !== '' &&  jobName !== newName) {
        changeJobName(jobName, newName);
    } else {
        showAlert('table-alert', 'info', 'Job name was not changed.');
        newName = jobName;
    }

    var td = $(this).parent();
    var tr = $(td).parent();

    td.remove();
    tr.prepend(jobNameTemplate({ jobName: newName }));
    bindEvents();

}

function bindEvents() {

    $('.job-delete').off('click', onJobDeleteClick);
    $('.job-delete').on('click', onJobDeleteClick);

    $('.edit-job').off('click', onEditJobClick);
    $('.edit-job').on('click', onEditJobClick);

    $('.save-job-name').off('click', onSaveJobClick);
    $('.save-job-name').on('click', onSaveJobClick);

    $('.submit-on-enter').off('keydown', submitOnEnter);
    $('.submit-on-enter').on('keydown', submitOnEnter);

}

function changeJobName(jobName, newName) {

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/edit_job',
        data: { job_name: jobName, name: newName },
        dataType: 'json',
        async: true,
        success: function() {
            $('tr[data-job="' + jobName + '"]').attr('data-job', newName);
            showAlert('table-alert', 'success', 'Job name changed successfully.');
        },
        error: function() {
            showAlert('table-alert', 'error', "There was a problem changing the job's name.");
        }
    });

    updateJobsData();

}

function deleteJob(jobName) {

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/delete_job',
        data: { job_name: jobName },
        dataType: 'json',
        success: function() {
            showAlert('table-alert', 'success', 'Job deleted successfully.');
            $('[data-job="' + jobName + '"]').remove();
            $('#new-job-name').val('');
        },
        error: function() {
            showAlert('table-alert', 'error', 'There was an error deleting the job.');
        },
        async: true
    });

}

function addNewJob(jobName) {

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/add_job',
        data: { job_name: jobName },
        dataType: 'json',
        success: function() {
            showAlert('new-alert', 'success', 'Job created successfully.');
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
                jobURL: $SCRIPT_ROOT + '/job/' + thisJob.job_id,
                exportURL: $SCRIPT_ROOT + '/api/export_job?job_name=' + thisJob.name
            })
        );
    }

    bindEvents();
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
                  clearTimeout(jobsDataTimeout);
                  jobsDataTimeout = setTimeout(updateJobsData, updateDataDelayMs);
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

            var descendants = $(this).children().clone(true);
            $(this).text('');
            if (job[attr] !== null) {
                $(this).text(job[attr]);
            }

            for (var i = 0; i < transforms.length; i++) {
                var transform = transforms[i];
                applyTransformation($(this), job[attr], transform);
            }

            $(this).append(descendants);

        });

    });

    bindEvents();

}
