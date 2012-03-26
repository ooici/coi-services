/**
 * Created by PyCharm.
 * User: luke
 * Date: 03/23/12
 * Time: 11:51
 * To change this template use File | Settings | File Templates.
 */
// Determines the x,y average between two nodes
// Dynamic resizing of the Chart Area
//todo: needs some tweaking
var width = 600, height = 270;
var m = [20, 120, 20, 120],
    w = width - m[1] - m[3],
    h = height - m[0] - m[2],
    i = 0,
    duration = 500,
    root;

var tree = d3.layout.tree()
    .size([h, w]);

var diagonal = d3.svg.diagonal()
    .projection(function(d) { return [d.y, d.x]; });

var vis = d3.select("#chart").append("svg")
    .attr("width", w + m[1] + m[3])
    .attr("height", h + m[0] + m[2])
    .append("g")
    .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

function avg(s, t) {
    return {x: (s.x + t.x)/2., y: (s.y + t.y)/2.};
}
// Debugging information
function debug(s) {
    d3.select("#debug").append("pre").text(s);
}
function build(resid) {
    d3.json("/tree/" + resid, function(json) {
        root = json;
        root.x0 = h / 2;
        root.y0 = 0;

        function collapse(d) {
            if (d.children) {
                d._children = d.children;
                d._children.forEach(collapse);
                d.children = null;
            }
        }

        root.children.forEach(collapse);
        update(root);
    });
}
function update(source) {

    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse();

    // Normalize for fixed-depth.
    // 180 per level
    nodes.forEach(function(d) { d.y = d.depth * 180; });

    // Update the nodes...
    var node = vis.selectAll("g.node")
        .data(nodes, function(d) { return d.id || (d.id = ++i); });

    // Enter any new nodes at the parent's previous position.
    var nodeEnter = node.enter().append("g")
        .attr("class", "node")
        .attr("transform", function(d) { return "translate(" + source.y0 + "," + source.x0 + ")"; })
        .on("click", click);
    // New nodes are created right over the the parent

    nodeEnter.append("circle")
        .attr("r", 1e-6)
        .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

    var hlink = nodeEnter.append("a")
        .attr("xlink:href",function(d) { return "/view/" + d.id;})

    hlink.append("text")
        .attr("x", function(d) { return d.children || d._children ? -10 : 10; })
        .attr("dy", ".35em")
        .attr("text-anchor", function(d) { return d.children || d._children ? "end" : "start"; })
        .text(function(d) { return d.name; })
        .style("fill-opacity", 1e-6);

    // Transition nodes to their new position.
    var nodeUpdate = node.transition()
        .duration(duration)
        .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

    nodeUpdate.select("circle")
        .attr("r", 4.5)
        .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

    nodeUpdate.select("text")
        .style("fill-opacity", 1);

    // Transition exiting nodes to the parent's new position.
    var nodeExit = node.exit().transition()
        .duration(duration)
        .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
        .remove();

    nodeExit.select("circle")
        .attr("r", 1e-6);

    nodeExit.select("text")
        .style("fill-opacity", 1e-6);

    // Update the labels
    var labels = vis.selectAll("text.label")
        .data(tree.links(nodes), function(d) { return d.target.id; });

    // Create the new labels using the average of the source/target x,y pairs
    // Initially have the labels opaque
    labels.enter().append("text").attr("class","label")
        .attr("dx",function(d) { return avg(d.source, d.target).y; })
        .attr("dy",function(d) { return avg(d.source, d.target).x; })
        .attr("text-anchor","middle")
        .style("fill-opacity",1e-6)
        .text(function(d) { debug(d.target.association); return d.target.association;});

    // Update the labels' position and opacity
    labels.transition().duration(duration)
        .attr("dx",function(d) { return avg(d.source, d.target).y; })
        .attr("dy",function(d) { return avg(d.source, d.target).x; })
        .style("fill-opacity",1);

    // Fade out and remove exiting labels
    labels.exit().transition().style("fill-opacity",1e-6).remove();

    // Update the links…
    var link = vis.selectAll("path.link")
        .data(tree.links(nodes), function(d) { return d.target.id; });

    // Enter any new links at the parent's previous position.
    link.enter().insert("path", "g")
        .attr("class", "link")
        .attr("d", function(d) {
            var o = {x: source.x0, y: source.y0};
            return diagonal({source: o, target: o});
        });

    // Transition links to their new position.
    link.transition()
        .duration(duration)
        .attr("d", diagonal);

    // Transition exiting nodes to the parent's new position.
    link.exit().transition()
        .duration(duration)
        .attr("d", function(d) {
            var o = {x: source.x, y: source.y};
            return diagonal({source: o, target: o});
        })
        .remove();

    // Stash the old positions for transition.
    nodes.forEach(function(d) {
        d.x0 = d.x;
        d.y0 = d.y;
    });
}

// Toggle children on click.
function click(d) {
    if (d.children) {
        d._children = d.children;
        d.children = null;
    } else {
        d.children = d._children;
        d._children = null;
    }
    update(d);
}

