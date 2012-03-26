/**
 * Created by PyCharm.
 * User: luke
 * Date: 03/22/12
 * Time: 15:19
 * To change this template use File | Settings | File Templates.
 */
function build(resid) {
    function avg(src, dst) {
        return [(src.x + dst.x) / 2., (src.y + dst.y) / 2];
    }
    var w = 900,
        h = 400;

    var tree = d3.layout.tree().size([h, w - 160]);

    var diagonal = d3.svg.diagonal().projection(function (d) {
        return [d.y, d.x];
    });

    var vis = d3.select("#chart")
        .append("svg")
        .attr("width", w)
        .attr("height", h)
        .append("g")
        .attr("class", "main")
        .attr("transform", "translate(100,0)");

    var debug = d3.select("#debug");


    d3.json("/tree/" + resid, function (json) {
        var nodes = tree.nodes(json);
        vis.selectAll("path.link")
            .data(tree.links(nodes))
            .enter()
            .append("path")
            .attr("class", "link")
            .attr("d", diagonal);

        var links = tree.links(nodes);
        for (var i in links) {
            var link = links[i];
            if ("association" in link.target) {
                vis.append("text")
                    .attr("class", "label")
                    .attr("dx", avg(link.source, link.target)[1])
                    .attr("dy", avg(link.source, link.target)[0])
                    .attr("text-anchor", "middle")
                    .text(link.target.association);
                debug.append("p")
                    .text(link.source.name + " " + link.target.association + " " + link.target.name);
            }

        }
        for (var i in nodes) {
            var node = nodes[i];
            var point = vis.append("g")
                .attr("class", "node")
                .attr("transform", "translate(" + node.y + "," + node.x + ")");
            point.append("circle").attr("r", 4.5);

            if ("id" in node) {
                point.append("a")
                    .attr("xlink:href", "/view/" + node.id)
                    .append("text")
                    .attr("dx", node.children? -4 : 8)
                    .attr("dy", -4)
                    .attr("text-anchor", node.children? "end" : "start")
                    .text(node.name);
            } else {
                point.append("text")
                    .attr("dx", node.children? -4 : 8)
                    .attr("dy", -4)
                    .attr("text-anchor", node.children? "end" : "start")
                    .text(node.name);
            }
        }
    });
}
