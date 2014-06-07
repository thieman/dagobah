$(function() {
    var path = location.pathname.substring(1);
    if ( path )
        $('#navbar-left li[id$="' + path + '"]').attr('class', 'active');
});
