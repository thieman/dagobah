var tasksTableHeadersTemplate = Handlebars.compile($('#tasks-table-headers-template').html());
var tasksTableResultsTemplate = Handlebars.compile($('#tasks-table-results-template').html());
var tasksTableCommandsTemplate = Handlebars.compile($('#tasks-table-commands-template').html());
var tasksTableTimeoutsTemplate = Handlebars.compile($('#tasks-table-timeouts-template').html());
var tasksTableRemoteTemplate = Handlebars.compile($('#tasks-table-remote-template').html());
var editTaskTemplate = Handlebars.compile($('#tasks-edit-template').html());

var tasksNameTemplate = Handlebars.compile($('#tasks-data-name-template').html());
var tasksCommandTemplate = Handlebars.compile($('#tasks-data-command-template').html());
var tasksSoftTimeoutTemplate = Handlebars.compile($('#tasks-data-soft-timeout-template').html());
var tasksHardTimeoutTemplate = Handlebars.compile($('#tasks-data-hard-timeout-template').html());
var tasksRemoteTargetTemplate = Handlebars.compile($('#tasks-data-remote-target-template').html());

Handlebars.registerPartial('tasksName', tasksNameTemplate);
Handlebars.registerPartial('tasksCommand', tasksCommandTemplate);
Handlebars.registerPartial('tasksSoftTimeout', tasksSoftTimeoutTemplate);
Handlebars.registerPartial('tasksHardTimeout', tasksHardTimeoutTemplate);
Handlebars.registerPartial('tasksRemoteTarget', tasksRemoteTargetTemplate);

Handlebars.registerHelper('equal', function(lvalue, rvalue, options) {
    if (arguments.length < 3)
        throw new Error("Handlebars Helper equal needs 2 parameters");
    if( lvalue!=rvalue ) {
        return options.inverse(this);
    } else {
    return options.fn(this);
    }
});

var fieldMap = {
    "Task": 'name',
    "Command": 'command',
    "Soft Timeout": 'soft_timeout',
    "Hard Timeout": 'hard_timeout',
    "Remote Target": 'hostname'
};

var fieldTemplateMap = {
    "Task": tasksNameTemplate,
    "Command": tasksCommandTemplate,
    "Soft Timeout": tasksSoftTimeoutTemplate,
    "Hard Timeout": tasksHardTimeoutTemplate,
    "Remote Target": tasksRemoteTargetTemplate
};

function runWhenJobLoaded() {
    if (typeof job != 'undefined' && job.loaded === true) {
        resetTasksTable();
        setInterval(updateJobStatusViews, 500);
        setInterval(updateJobNextRun, 500);
        setInterval(updateTasksTable, 500);
    } else {
        setTimeout(runWhenJobLoaded, 50);
    }
}

runWhenJobLoaded();

function onTaskDeleteClick() {
    $(this).parents('[data-task]').each(function() {
        deleteTask($(this).attr('data-task'));
    });
}

function onEditTaskClick() {
    var td = $(this).parent();
    var tr = $(td).parent();

    $(td).children().remove();
    var original = $(td).text();

    var index = $(tr).children('td').index(td);
    var field = $('#tasks-headers > th:eq(' + index + ')').text();
    td.remove();

    if (index === 0) {
      tr.prepend(editTaskTemplate({ original: original, field: field, knownHosts: knownHosts }));
    } else {
        $(tr).children().each(function() {
            if ( $(tr).children().index(this) === (index - 1) ) {
                $(this).after(editTaskTemplate({
                    original: original,
                    field: field,
                    knownHosts: knownHosts
                }));
            }
        });
    }

    $(tr).find('>:nth-child(' + (index + 1) + ')').find('input').select();
    bindEvents();

}

function editTask(taskName, field, newValue) {

    if (!job.loaded) {
        return;
    }

    var postData = {job_name: job.name, task_name: taskName};
    postData[fieldMap[field]] = newValue;

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/edit_task',
        data: postData,
        dataType: 'json',
        async: true,
        success: function() {
            if (fieldMap[field] === 'name') {
                $('tr[data-task="' + taskName + '"]').attr('data-task', newValue);
                job.renameTask(taskName, newValue);
            }
            showAlert('table-alert', 'success', 'Task changed successfully.');
        },
        error: function() {
            showAlert('table-alert', 'error', "There was a problem changing the task's information.");
        }
    });

}

function onSaveTaskEditClick() {

    var input = $(this).siblings('[data-field]');
    var field = $(input).attr('data-field');
    var original = $(input).attr('data-original');
    var newValue = $(input).val();

    var td = $(this).parent();
    var tr = $(td).parent();
    var index = $(tr).children('td').index(td);

    var taskName = $(tr).attr('data-task');

    if (original !== null &&  original !== newValue) {
        if (field == "Remote Target" || newValue !== '') {
            editTask(taskName, field, newValue);
        }
    } else {
        showAlert('table-alert', 'info', 'Task was not changed.');
        newValue = original;
    }

    td.remove();

    var template = fieldTemplateMap[field];

    if (index === 0) {
        tr.prepend(template({ text: newValue }));
    } else {
        $(tr).children().each(function() {
            if ( $(tr).children().index(this) === (index - 1) ) {
                $(this).after(template({ text: newValue }));
            }
        });
    }

    bindEvents();

}

function bindEvents() {

    $('.task-delete').off('click', onTaskDeleteClick);
    $('.task-delete').on('click', onTaskDeleteClick);

    $('.edit-task').off('click', onEditTaskClick);
    $('.edit-task').on('click', onEditTaskClick);

    $('.save-task-edit').off('click', onSaveTaskEditClick);
    $('.save-task-edit').on('click', onSaveTaskEditClick);

    $('.submit-on-enter').off('keydown', submitOnEnter);
    $('.submit-on-enter').on('keydown', submitOnEnter);

}

function deleteDependency(fromTaskName, toTaskName) {

    if (!job.loaded) {
        return;
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/delete_dependency',
        data: {
            job_name: job.name,
            from_task_name: fromTaskName,
            to_task_name: toTaskName
        },
        dataType: 'json',
        async: true,
        success: function() {
            job.removeDependencyFromGraph(fromTaskName, toTaskName);
            showAlert('graph-alert', 'success', 'Dependency from ' +
                    fromTaskName + ' to ' + toTaskName +
                    ' was successfully removed.');
        },
        error: function() {
            showAlert('graph-alert', 'error', 'There was an error removing this dependency.');
        }
    });

}

function deleteTask(taskName, alertId) {

    if (!job.loaded) {
        return;
    }

    if (typeof alertId === 'undefined') {
        alertId = 'table-alert';
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/delete_task',
        data: {
            job_name: job.name,
            task_name: taskName
        },
        dataType: 'json',
        async: true,
        success: function() {
            job.update(function() {
                resetTasksTable();
                job.removeTaskFromGraph(taskName);
            });
            showAlert(alertId, 'success', 'Task ' + taskName + ' deleted.');
        },
        error: function() {
            showAlert(alertId, 'error', 'There was an error deleting the task.');
        },
        dataType: 'json'
    });

}

$('#remote_checkbox').click(function () {
    $("#target_hosts").toggle(this.checked);
});

$('#add-task').click(function() {

    var newName = $('#new-task-name').val();
    var newCommand = $('#new-task-command').val();
    if ($('#remote_checkbox').is(':checked')){
        var newTargetHostId = $('#target-hosts-dropdown').val();
    }

    if (newName === null || newName === '') {
        showAlert('new-alert', 'error', 'Please enter a name for the new task.');
        return;
    }
    if (newCommand === null || newCommand === '') {
        showAlert('new-alert', 'error', 'Please enter a command for the new task.');
        return;
    }

    addNewTask(newName, newCommand, newTargetHostId);

});

function addNewTask(newName, newCommand, newTargetHostId) {

    if (!job.loaded) {
        return;
    }

    if (newTargetHostId) {
        data = {
            job_name: job.name,
            task_name: newName,
            task_command: newCommand,
            task_target: newTargetHostId
        };
    } else {
        data = {
            job_name: job.name,
            task_name: newName,
            task_command: newCommand
        };
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/add_task_to_job',
        data: data,
        dataType: 'json',
        success: function() {
            showAlert('new-alert', 'success', 'Task added to job.');
            job.update(function() {
                job.addTaskToGraph(newName);
                resetTasksTable();
            });
            $('#new-task-name').val('');
            $('#new-task-command').val('');
            $('#target-hosts-dropdown').val('');
        },
        error: function() {
            showAlert('new-alert', 'error', 'There was an error adding the task to this job.');
        },
        async: true
    });

}

function resetTasksTable(tableMode) {

    if (!job.loaded) {
        return;
    }

    if (typeof tableMode === 'undefined') {
        tableMode = getTableMode();
    }

    $('#tasks-headers').empty();
    $('#tasks-body').empty();

    var headers=[];
    if (tableMode === 'results') {
        headers = ['Task', 'Started', 'Completed', 'Result', ''];
    } else if (tableMode === 'commands') {
        headers = ['Task', 'Command', ''];
    } else if (tableMode === 'timeouts') {
        headers = ['Task', 'Soft Timeout', 'Hard Timeout', ''];
    } else if (tableMode === 'remote') {
        headers = ['Task', 'Remote Target', ''];
    }

    for (var i = 0; i < headers.length; i++) {
        $('#tasks-headers').append(
            tasksTableHeadersTemplate({
                headerName: headers[i]
            })
        );
    }

    for (var i = 0; i < job.tasks.length; i++) {
        var thisTask = job.tasks[i];

        if (tableMode === 'results') {
            $('#tasks-body').append(
                tasksTableResultsTemplate({
                    taskName: thisTask.name,
                    taskURL: $SCRIPT_ROOT + '/job/' + job.id + '/' + thisTask.name
                })
            );
        } else if (tableMode === 'commands') {
            $('#tasks-body').append(
                    tasksTableCommandsTemplate({
                    taskName: thisTask.name,
                    taskURL: $SCRIPT_ROOT + '/job/' + job.id + '/' + thisTask.name
                })
            );
        } else if (tableMode === 'timeouts') {
            $('#tasks-body').append(
                tasksTableTimeoutsTemplate({
                    taskName: thisTask.name,
                    taskURL: $SCRIPT_ROOT + '/job/' + job.id + '/' + thisTask.name
                })
            );
        } else if (tableMode === 'remote') {
            $('#tasks-body').append(
                    tasksTableRemoteTemplate({
                    taskName: thisTask.name,
                    taskURL: $SCRIPT_ROOT + '/job/' + job.id + '/' + thisTask.name
                })
            );
        }

    }

    bindEvents();
    updateTasksTable();

}

function getTableMode() {
    return $('#table-toggle').children('.active').val();
}

$('#table-toggle').children().click(function() {
    resetTasksTable($(this).val());
});

function updateTasksTable() {

    if (!job.loaded) {
        return;
    }

    $('#tasks-body').children().each(function() {

        var taskName = $(this).attr('data-task');
        for (var i = 0; i < job.tasks.length; i++) {
            if (job.tasks[i].name === taskName) {
                var task = job.tasks[i];
                break;
            }
        }

        $(this).find('[data-attr]').each(function() {

            var attr = $(this).attr('data-attr');
            var transform = $(this).attr('data-transform');

            var descendants = $(this).children().clone(true);

            $(this).text('');
            if (task[attr] !== null) {
                $(this).text(task[attr]);
            }

            if (typeof transform === 'undefined' || transform === false) {
                // no transform attribute
            } else {
                applyTransformation($(this), task[attr], transform);
            }

            $(this).append(descendants);

        });

    });

}

function updateJobStatusViews() {

    if (!job.loaded) {
        return;
    }

    setControlButtonStates();
    $('#job-status')
        .removeClass('status-waiting status-running status-failed')
        .addClass('status-' + job.status)
        .text(toTitleCase(job.status));

}

function setControlButtonStates() {
    // disable control buttons based on current job state

    if (!job.loaded) {
        return;
    }

    $('#start-job').prop('disabled', false);
    $('#retry-job').prop('disabled', false);
    $('#terminate-job').prop('disabled', false);
    $('#kill-job').prop('disabled', false);

    if (job.status == 'waiting') {
        $('#terminate-job').prop('disabled', true);
        $('#kill-job').prop('disabled', true);
    } else if (job.status == 'running') {
        $('#start-job').prop('disabled', true);
    } else if (job.status == 'failed') {
        $('#terminate-job').prop('disabled', true);
        $('#kill-job').prop('disabled', true);
    }

}

function updateJobNextRun() {
    if (!job.loaded) {
        return;
    }

    if (job.next_run === null) {
        $('#next-run').val('Not scheduled');
    } else {
        $('#next-run').val(moment.utc(job.next_run).local().format('LLL'));
    }
}

$('#save-notes').click(function() {
    var notes = $('#job-notes').val();
    updateNotes(notes);
});

function updateNotes(newNotes) {
    if (!job.loaded) {
        return;
    }

    data = {
        job_name: job.name,
        notes: newNotes,
    };

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/update_job_notes',
        data: data,
        dataType: 'json',
        success: function() {
            showAlert('notes-alert', 'success', 'Notes updated.');
        },
        error: function() {
            showAlert('notes-alert', 'error', 'There was an error updating notes.');
        },
        async: true
    });
}

function updateSchedule(jobName, cronSchedule) {

    if (!job.loaded) {
        return;
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/schedule_job',
        data: {
            job_name: jobName,
            cron_schedule: cronSchedule
        },
        dataType: 'json',
        success: function () {
            successMsg = cronSchedule === "" ? 'Job unscheduled successfully' : 'Job scheduled successfully';
            showAlert('schedule-alert', 'success', successMsg);
            updateJobNextRun();
        },
        error: function() {
            errorMsg = cronSchedule === "" ? 'Failed to unschedule job' : 'Failed to schedule job';
            showAlert('schedule-alert', 'error', errorMsg);
        },
        async: true
    });

}

$('#save-schedule').click(function() {
  updateSchedule(job.name, $('#cron-schedule').val());
});

$('#clear-schedule').click(function() {
  $('#cron-schedule').val('None');
  updateSchedule(job.name, '');
});

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
            showAlert('state-alert', 'success', 'Retrying failed tasks');
        },
        error: function() {
            showAlert('state-alert', 'error', 'Unable to retry failed tasks');
        },
        async: true
    });

});

$('#terminate-job').click(function() {

    if (!job.loaded) {
        return;
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/terminate_all_tasks',
        data: {job_name: job.name},
        dataType: 'json',
        success: function() {
            showAlert('state-alert', 'success', 'All running tasks terminated');
        },
        error: function() {
            showAlert('state-alert', 'error', 'Unable to terminate some or all tasks');
        },
        async: true
    });

});

$('#kill-job').click(function() {

    if (!job.loaded) {
        return;
    }

    $.ajax({
        type: 'POST',
        url: $SCRIPT_ROOT + '/api/kill_all_tasks',
        data: {job_name: job.name},
        dataType: 'json',
        success: function() {
            showAlert('state-alert', 'success', 'All running tasks killed');
        },
        error: function() {
            showAlert('state-alert', 'error', 'Unable to kill some or all tasks');
        },
        async: true
    });

});

$('.toggle-help').click(function() {
    $('.chart-help').toggleClass('hidden');
});
