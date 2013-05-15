// set up SVG for D3
var width  = 940,
    height = 600,
    colors = d3.scale.category10();

var svg = d3.select('.job-graph')
  .append('svg')
  .attr('width', width)
  .attr('height', height);

var job = new Job();
job.load(jobName);
drawIfLoaded();

function drawIfLoaded() {
	if (job.loaded) {
		drawForceGraph();
	} else {
		window.setTimeout(drawIfLoaded, 50);
	}
}

function drawForceGraph() {

	var nodes = job.getForceNodes();
	var links = job.getForceLinks();

    // init D3 force layout
    var force = d3.layout.force()
        .nodes(nodes)
        .links(links)
        .size([width, height])
        .linkDistance(75)
        .charge(-1500)
        .on('tick', tick);

    // define arrow markers for graph links
    svg.append('svg:defs').append('svg:marker')
        .attr('id', 'end-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 30)
        .attr('markerWidth', 4)
        .attr('markerHeight', 4)
        .attr('orient', 'auto')
      .append('svg:path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#000');

    // line displayed when dragging new nodes
    var drag_line = svg.append('svg:path')
      .attr('class', 'link dragline hidden')
      .attr('d', 'M0,0L0,0');

    // handles to link and node element groups
    var path = svg.append('svg:g').selectAll('path'),
        circle = svg.append('svg:g').selectAll('g');

    // mouse event vars
    var selected_node = null,
        selected_link = null,
        mousedown_link = null,
        mousedown_node = null,
        mouseup_node = null;

    function resetMouseVars() {
      mousedown_node = null;
      mouseup_node = null;
      mousedown_link = null;
    }

    // update force layout (called automatically each iteration)
    function tick() {
      // draw directed edges with proper padding from node centers
      path.attr('d', function(d) {
        var deltaX = d.target.x - d.source.x,
            deltaY = d.target.y - d.source.y,
            dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
            normX = deltaX / dist,
            normY = deltaY / dist,
            sourcePadding = d.left ? 15 : 10,
            targetPadding = d.right ? 15 : 10,
            sourceX = d.source.x + (sourcePadding * normX),
            sourceY = d.source.y + (sourcePadding * normY),
            targetX = d.target.x - (targetPadding * normX),
            targetY = d.target.y - (targetPadding * normY);
        return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
      });

      circle.attr('transform', function(d) {
        return 'translate(' + d.x + ',' + d.y + ')';
      });
    }

    // update graph (called when needed)
    function restart() {

      // path (link) group
      path = path.data(links);

      // update existing links
      path.classed('selected', function(d) { return d === selected_link; })
        .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
        .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; });


      // add new links
      path.enter().append('svg:path')
        .attr('class', 'link')
        .classed('selected', function(d) { return d === selected_link; })
        .style('marker-start', function(d) { return d.left ? 'url(#start-arrow)' : ''; })
        .style('marker-end', function(d) { return d.right ? 'url(#end-arrow)' : ''; })
        .on('mousedown', function(d) {
          if(d3.event.ctrlKey) return;

          // select link
          mousedown_link = d;
          if(mousedown_link === selected_link) selected_link = null;
          else selected_link = mousedown_link;
          selected_node = null;
          restart();
        });

      // remove old links
      path.exit().remove();

      // circle (node) group
      circle = circle.data(nodes, function(d) { return d.id; });

      // update existing nodes
      circle.selectAll('circle')
        .style('fill', function(d) { return (d === selected_node) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id); });

      // add new nodes
      var g = circle.enter().append('svg:g');

	  g.append('svg:rect')
		.attr('height', 25)
		.attr('width', 100)
		.attr('x', -50)
		.attr('y', -12.5)
        .attr('class', function(d) {
			return 'node';
		})
        .style('fill', function(d) { return (d === selected_node) ? d3.rgb(colors(d.id)).brighter().toString() : colors(d.id); })
        .style('stroke', function(d) { return d3.rgb(colors(d.id)).darker().toString(); })
        .on('mouseover', function(d) {
          if(!mousedown_node || d === mousedown_node) return;
          // enlarge target node
          d3.select(this).attr('transform', 'scale(1.1)');
        })
        .on('mouseout', function(d) {
          if(!mousedown_node || d === mousedown_node) return;
          // unenlarge target node
          d3.select(this).attr('transform', '');
        })
        .on('mousedown', function(d) {
          if(d3.event.ctrlKey) return;

          // select node
          mousedown_node = d;
          if(mousedown_node === selected_node) selected_node = null;
          else selected_node = mousedown_node;
          selected_link = null;

          // reposition drag line
          drag_line
            .style('marker-end', 'url(#end-arrow)')
            .classed('hidden', false)
            .attr('d', 'M' + mousedown_node.x + ',' + mousedown_node.y + 'L' + mousedown_node.x + ',' + mousedown_node.y);

          restart();
        })
        .on('mouseup', function(d) {
          if(!mousedown_node) return;

          // needed by FF
          drag_line
            .classed('hidden', true)
            .style('marker-end', '');

          // check for drag-to-self
          mouseup_node = d;
          if(mouseup_node === mousedown_node) { resetMouseVars(); return; }

          // unenlarge target node
          d3.select(this).attr('transform', '');

          // add link to graph (update if exists)
          // NB: links are strictly source < target; arrows separately specified by booleans
          var source, target, direction;
          if(mousedown_node.id < mouseup_node.id) {
            source = mousedown_node;
            target = mouseup_node;
            direction = 'right';
          } else {
            source = mouseup_node;
            target = mousedown_node;
            direction = 'left';
          }

          var link;
          link = links.filter(function(l) {
            return (l.source === source && l.target === target);
          })[0];

          if(link) {
            link[direction] = true;
          } else {
            link = {source: source, target: target, left: false, right: false};
            link[direction] = true;
            links.push(link);
          }

          // select new link
          selected_link = link;
          selected_node = null;
          restart();
        });

      // show node IDs
      g.append('svg:text')
          .attr('x', 0)
          .attr('y', 4)
          .attr('class', 'id')
          .text(function(d) { return d.id; });

      // remove old nodes
      circle.exit().remove();

      // set the graph in motion
      force.start();
    }

    function mousedown() {
      // prevent I-bar on drag
      //d3.event.preventDefault();

      // because :active only works in WebKit?
      svg.classed('active', true);

      if(d3.event.ctrlKey || mousedown_node || mousedown_link) return;

      // insert new node at point
      var point = d3.mouse(this),
          node = {id: 'New Task'};
      node.x = point[0];
      node.y = point[1];
      nodes.push(node);

      restart();
    }

    function mousemove() {
      if(!mousedown_node) return;

      // update drag line
      drag_line.attr('d', 'M' + mousedown_node.x + ',' + mousedown_node.y + 'L' + d3.mouse(this)[0] + ',' + d3.mouse(this)[1]);

      restart();
    }

    function mouseup() {
      if(mousedown_node) {
        // hide drag line
        drag_line
          .classed('hidden', true)
          .style('marker-end', '');
      }

      // because :active only works in WebKit?
      svg.classed('active', false);

      // clear mouse event vars
      resetMouseVars();
    }

    function spliceLinksForNode(node) {
      var toSplice = links.filter(function(l) {
        return (l.source === node || l.target === node);
      });
      toSplice.map(function(l) {
        links.splice(links.indexOf(l), 1);
      });
    }

    // app starts here
    svg.on('mousedown', mousedown)
      .on('mousemove', mousemove)
      .on('mouseup', mouseup);
    restart();

}
