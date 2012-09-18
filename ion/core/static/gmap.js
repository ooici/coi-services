var map;
var points = {};
function getUrlVars() {
    var vars = {};
    var parts = window.location.href.replace(/[?&]+([^=&]+)=([^&]*)/gi, function(m,key,value) {
        vars[key] = value;
    });
    return vars;
}
function initMap() {
    var buffer = null;
    var rbuffer = null;
    var myOptions = {
        zoom: 5,
        center: new google.maps.LatLng(41,-71),
        mapTypeId: google.maps.MapTypeId.SATELLITE
    };
    map = new google.maps.Map(document.getElementById('map_canvas'), myOptions);
    google.maps.event.addListener(map, 'click', function(x) {
        update();
        if (buffer != x) {
            var lat = x.latLng.lat();
            var lon = x.latLng.lng();
            console.debug([lat,lon]);
            buffer = x;
            var e = event;
            if (e.ctrlKey)
                console.debug("Ctrl-click");
            if (e.shiftKey)
                copyToClipboard("[" + lat + "," + lon + "]");
        }

    });
    google.maps.event.addListener(map,'rightclick', function(x) {
        if(rbuffer!= x) {
            var lat = x.latLng.lat();
            var lon = x.latLng.lng();
            window.open("http://localhost:8080/new/PlatformDevice?index_location.lat=" + lat + "&index_location.lon=" + lon, "_blank");
            rbuffer = x;
        }
    });
}


function initialize(results) {
    results.forEach(function(x) {
        var name = x.name;
        console.debug(x.name);
        if(! points[name]) {
            var markerLoc = new google.maps.LatLng(x.index_location.lat, x.index_location.lon);
            var marker = new google.maps.Marker({
                position: markerLoc,
                map: map,
                title: "Name: " + x.name + "\nDescrtiption: " + x.description + "\nModel:" + x.model + "\nLat: " + x.index_location.lat + "\nLon: " + x.index_location.lon,
                //icon: "http://a1.twimg.com/profile_images/662211540/Oil_Rig_normal.PNG",
            });
            google.maps.event.addListener(marker, 'click', function(){
                console.debug(x);
                window.open("http://localhost:8080/view/" + x._id, "_blank");
            });
            points[name]=true;
        }
    });

};
function copyToClipboard (text) {
    window.prompt ("Copy to clipboard: Ctrl+C, Enter", text);
}
function update() {
    var results = [];
    var data;
    var general_request = {"query":{"match_all":{}}, "filter":{"exists":{"field":"index_location"}, "limit":1000}};
    data = general_request;
    var urlVars = getUrlVars();
    if("distance" in urlVars) {
        narrow_request['filter']['geo_distance']['distance'] = urlVars['distance'] + 'km' ;
        data = narrow_request;
    }
    $.ajax({
        url: "http://localhost:8080/esquery",
        type: "POST",
        contentType: "application/json;charset=utf-8",
        data: JSON.stringify(data),
        dataType: "json",
        beforeSend: function(x) {
            if ( x && x.overrideMimeType) {
                x.overrideMimeType("application/json;charset=utf-8");

            }
        },
        success: function(data) {
            data.hits.hits.forEach(function(x) { results.push(x._source); } );
            initialize(results);
        }

    });

}

$(document).ready(function() { initMap(); update(); });
