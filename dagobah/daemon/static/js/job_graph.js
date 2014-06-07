// set up SVG for D3
var width  = 940,
    height = 600,
    gWidth = width * 5,
    gHeight = height * 5,
    nodeWidth = 150,
    nodeHeight = 50,
    colors = d3.scale.category10();

var svg = d3.select('.job-graph')
        .append('svg')
        .attr('width', width)
        .attr('height', height)
        .attr('pointer-events', 'all')
        .append('g')
        .call(d3.behavior.zoom().on('zoom', zoomGraph))
        .attr('transform', 'translate(' + (width / 2) + ',' + (height / 2) + ') scale(' + getInitialZoom() + ')')
        // .on('mousedown.zoom', null)   // comment this out to allow pan
        .append('g');

// this is for the zoom functionality
svg.append('rect')
    .attr('x', gWidth * -0.5)
    .attr('y', gHeight * -0.5)
    .attr('width', gWidth)
    .attr('height', gHeight)
    .attr('fill', 'transparent')
    .style('stroke', '#CCCCCC')
    .style('stroke-width', '1px');

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

function getInitialZoom() {
    return 0.8;
}

function zoomGraph(d) {
    svg.attr('transform', 'translate(' + d3.event.translate + ')' + ' scale(' + d3.event.scale +')');
}

function panGraph(d) {
    svg.attr('transform', '');
}

var nodes = [];
var links = [];
var restartGraph = null;

function drawForceGraph() {

    nodes = job.getForceNodes();
    links = job.getForceLinks();
    var preRenderTicks = 100;
    var renderDelayMs = 750;
    var graphUpdateMs = 1000;

    // init D3 force layout
    var force = d3.layout.force()
            .nodes(nodes)
            .links(links)
            .size([1, 1])
            .linkDistance(200)
            .charge(-4000);

    // define arrow markers for graph links
    svg.append('svg:defs').append('svg:marker')
        .attr('id', 'end-arrow')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 75)
        .attr('markerWidth', 5)
        .attr('markerHeight', 5)
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
        mouseup_node = null,
        graphFocused = null;

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

    // get the graph in a non-chaotic state before rendering
    for (var i = 0; i < preRenderTicks; i++) {
        tick();
    }

    setTimeout(function() { force.on('tick', tick); }, renderDelayMs);
    setInterval(function() { job.update(function() {
        updateForceNodes(nodes);
        updateForceLinks(force.links());
    }); }, graphUpdateMs);

    function updateForceNodes(forceNodes) {

        for (var i = 0; i < forceNodes.length; i++) {
            var node = forceNodes[i];
            for (var j = 0; j < job.tasks.length; j++) {
                var task = job.tasks[j];
                if (task.name === node.id) {
                    node.status = job.forceNode(task.name).status;
                    break;
                }
            }
        }

        force.nodes(forceNodes);

        d3.selectAll('.node-element')
            .data(force.nodes())
            .select('.node')
            .attr('class', function(d) { return 'node ' + d.status; })
            .classed('selected', function(d) { return d === selected_node; });

        d3.selectAll('.node-element')
            .data(force.nodes())
            .select('p')
            .text(function(d) { return d.id; });

    }

    function updateForceLinks(forceLinks) {
    }

    // update graph (called when needed)
    function restart(charge, linkDistance) {

        if (typeof charge !== 'undefined') {
            force.charge(charge);
        }

        if (typeof linkDistance !== 'undefined') {
            force.linkDistance(linkDistance);
        }

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

        // add new nodes
        var g = circle.enter()
            .append('svg:g')
            .attr('class', 'node-element');

        g.append('svg:rect')
            .attr('class', function(d) { return 'node ' + d.status; })
            .classed('selected', function(d) { return d === selected_node; })
            .attr('height', nodeHeight)
            .attr('width', nodeWidth)
            .attr('x', -1 * (nodeWidth / 2))
            .attr('y', -1 * (nodeHeight / 2));

        g.append('foreignObject')
            .call(d3.behavior.zoom().on('zoom', null))
            .attr('class', 'node-object')
            .attr('x', -1 * (nodeWidth / 2))
            .attr('y', -1 * (nodeHeight / 2))
            .attr('width', nodeWidth)
            .attr('height', nodeHeight)
            .append('xhtml:body')
            .style('background-color', 'transparent')
            .on('mouseover', function(d) {
                if(!mousedown_node || d === mousedown_node) return;
                // enlarge target node
                d3.select(this).attr('transform', 'scale(1.1)');
            })
            .on('mouseout', function(d) {
                if(!mousedown_node || d === mousedown_node) return;
                // unenlarge target node
                d3.select(this).attr('transform', '');p
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
                    .style('marker-end', 'url(#end-arrow)');

                // check for drag-to-self
                mouseup_node = d;
                if(mouseup_node === mousedown_node) { resetMouseVars(); return; }
                if(mouseup_node !== mousedown_node) { selected_node = null; }

                // unenlarge target node
                d3.select(this).attr('transform', '');

                // add link to graph (update if exists)
                // NB: links are strictly source < target; arrows separately specified by booleans
                var source, target, direction;
                source = mousedown_node;
                target = mouseup_node;
                direction = 'right';

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
                    job.addDependency(link);
                }

                // select new link
                selected_link = link;
                selected_node = null;
                restart();
            })
            .append('p')
            .style('vertical-align', 'middle')
            .style('text-align', 'center')
            .style('margin-top', -1 * nodeHeight + 'px')
            .text(function(d) { return d.id; });

        circle.select('rect')
            .classed('selected', function(d) { return d === selected_node; });

        circle.select('p')
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
                .style('marker-end', 'url(#end-arrow)');
        }

        // because :active only works in WebKit?
        svg.classed('active', false);

        // clear mouse event vars
        resetMouseVars();
    }

    function mouseover() {
        graphFocused = true;
    }

    function mouseout() {
        graphFocused = false;
    }

    function keydown() {
        if (!graphFocused) return;
        if (!selected_node && !selected_link) return;
        switch (d3.event.keyCode) {
        case 68: // d
            if (selected_link) {
                deleteDependency(selected_link.source.id, selected_link.target.id);
            }
            if (selected_node) {
                deleteTask(selected_node.id, 'graph-alert');
            }
        }
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
        .on('mouseup', mouseup)
        .on('mouseover', mouseover)
        .on('mouseout', mouseout);

    d3.select('body').on('keydown', keydown);

    restartGraph = restart;
    restart();

}
