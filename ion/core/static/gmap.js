/**
 * Created with PyCharm.
 * User: luke
 * Date: 06/07/12
 * Time: 15:44
 * To change this template use File | Settings | File Templates.
 */
function getUrlVars() {
    var vars = {};
    var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m,key,value) {
        vars[key] = value;
    });
    return vars;
}

function initialize(results) {

    var map;
    var myOptions = {
        zoom: 5,
        center: new google.maps.LatLng(41,-71),
        mapTypeId: google.maps.MapTypeId.SATELLITE
    };
    map = new google.maps.Map(document.getElementById('map_canvas'), myOptions);
    
    results.forEach(function(x) {
        var markerLoc = new google.maps.LatLng(x.nominal_location.lat, x.nominal_location.lon);
        var marker = new google.maps.Marker({
            position: markerLoc,
            map: map,
            title: "Name: " + x.name + "\nModel:" + x.model + "\nLat: " + x.nominal_location.lat + "\nLon: " + x.nominal_location.lon,
            //icon: "http://a1.twimg.com/profile_images/662211540/Oil_Rig_normal.PNG",
        });
        google.maps.event.addListener(marker, 'click', function(){
            window.location.replace("http://localhost:8080/view/" + x._id);
        });
    });

};
function onLoad() {
//$(document).ready(function() {
    var results = [];
    var data;
    var request = {
        "query" : {
            "match_all":{}
        },
        "filter" : {
            "exists" : {
                "field" : "nominal_location"
            }
        }
    };
    var geo_request = {"query":{"match_all":{}}, "filter":{"geo_distance":{"nominal_location":[-71,41], "distance":"2000km"}}};
    var wide_request = {"query": {"match_all": {}}, "size": 100};
    var narrow_request = {"size" : 100, "filter": {"geo_distance": {"distance_type": "arc", "nominal_location": [-71,41], "distance": "2000km"}}, "query": {"match_all": {}}};
    data = request;
    var urlVars = getUrlVars();
    if("distance" in urlVars) {
        narrow_request['filter']['geo_distance']['distance'] = urlVars['distance'] + 'km' ;
        data = narrow_request;
    }
    $.ajax({
        url: "http://localhost:9200/_search",
        type: "POST",
        data: JSON.stringify(data),
        dataType: "json",
        beforeSend: function(x) {
            if ( x && x.overrideMimeType) {
                x.overrideMimeType("application/j-son;charset=UTF-8");

            }
        },
        success: function(data) {
            data.hits.hits.forEach(function(x) { results.push(x._source); } );
            initialize(results);
        }

    });

//});
}
