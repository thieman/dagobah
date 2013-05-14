(function($){  
  $('body').append('<div id="growlContainer" style="position:fixed;width:250px;top:20px;right:20px;bottom:0;overflow:auto;pointer-events:none;z-index:1051;"></div>')
  $.extend({
    growl: function(options) {  

      // OPTS
      // ====
      //
      // REQUIRED 
      // --------
      // text: The html/text content to show under the title or by itself.
      //
      // OPTIONAL
      // --------
      // title: Bold-faced html/text content to appear to the left of the text.
      // type: Either success/warning/info/error (Info is default)
      // delay: Milliseconds before growl disappears. (0 is default and creates
      // a dismissable growl.)
      // container: css-formatted (e.g. ".container") container in which to
      // populate growls. Default is "#growlContainer"
    
      var defaults = {  
        delay: 0
        , type: 'warning'
        , container: '#growlContainer'
        , text: ''
        , class: ''
      };  
      var _ = $.extend(defaults, options);  

      // FORMATTING
      _.fTitle =  ( _.title ? '<strong>' + _.title + '</strong>' : '')

      // UNIQUE IDENTIFICATION
      this.id = Math.floor(Math.random()*1000)

      // CREATE FULL DOM OBJECT
      this.html = document.createElement('div')
      this.html.className = 'growl' + this.id + ' alert alert-' + _.type + ' ' + _.class
      var xHtml = '<a style="pointer-events: auto;" class="close" data-dismiss="alert" href="#">&times;</a>'
      $(this.html).html( 
        (_.delay == 0 ? xHtml : '') 
        + _.fTitle + ' ' + _.text
      )

      // APPEND ALERT TO CONTAINER
      $(_.container).prepend(this.html)

      // SET DELAYS
      if ( _.delay > 0 ) { 
        var fn = (function(delay, id) { 
          setTimeout(function() {
            $('.growl' + id).fadeOut()
            }
            , delay
          )
        })(_.delay, this.id) 
      }
    }
  })  
})(jQuery);
