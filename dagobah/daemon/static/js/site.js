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

function applyTransformation(target, value, transformation) {

    if (transformation === 'datetime') {
        if (value !== null) {
            $(target).text(moment.utc(value).local().format('lll'));
        }
    } else if (transformation === 'class') {
        if (value !== null) {
            $(target).removeClass().addClass(value);
        }
    } else if (transformation === 'title') {
        if (value !== null) {
            $(target).text(toTitleCase(value));
        }
    } else if (transformation === 'length') {
        if (value !== null) {
            $(target).text(value.length);
        }
    } else if (transformation === 'class-success') {

        if (value === true) {
            $(target).text('Success');
        } else if (value === false) {
            $(target).text('Failed');
        }

        $(target).removeClass();
        if (value === true) {
            $(target).addClass('task-success');
        } else {
            $(target).addClass('task-failed');
        }

    }

}

function submitOnEnter(e) {
    var key = (e.keyCode ? e.keyCode : e.which);
    if (key === 13) {
        $(e.target).siblings('button').each(function() {
            $(this).click();
        });
    }
}
