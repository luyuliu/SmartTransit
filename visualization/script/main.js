$("#sidebar-hide-btn").click(function () {
  animateSidebar();
  $('.mini-submenu').fadeIn();
  return false;
});


$('.mini-submenu').on('click', function () {
  animateSidebar();
  $('.mini-submenu').hide();
})

function animateSidebar() {
  $("#sidebar").animate({
    width: "toggle"
  }, 350, function () {
    map.invalidateSize();
  });
}

function switchStatus(status, line) {
  switch (status) {
    case 0:
      line.zero_count++;

      break;

    case 1:
      line.one_count++;

      break;

    case 2:
      line.two_count++;

      break;

    default:
      line.miss_count++;
  }
  line.total_count++;
}


var baseLayer = L.esri.basemapLayer('Topographic')
map = L.map("map", {
  zoom: 13,
  center: [39.98, -83],
  layers: [baseLayer],
  zoomControl: false,
  attributionControl: false,
  maxZoom: 18
});

new L.Control.Zoom({ position: 'topright' }).addTo(map);

var arrow = L.polyline([[57, -19], [60, -12]], {}).addTo(map);
/*
var arrowHead = L.polylineDecorator(arrow, {
  patterns: [
    { offset: '100%', repeat: 0, symbol: L.Symbol.arrowHead({ pixelSize: 15, polygon: false, pathOptions: { stroke: true } }) }
  ]
}).addTo(map);*/


stopsLayer = new L.markerClusterGroup({
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
  zoomToBoundsOnClick: true,
  disableClusteringAtZoom: 15,
});




$(document).ready(function () {
  $('#date-input').val(("2018-01-31"))
})




var tran;

function zoomIn(){
  var zoomLevel = parseFloat($("#zoom-input").val())
  console.log(zoomLevel)
  map.setZoom(zoomLevel);
}

$("#start-btn").click(function () {
  todayDate = $("#date-input").val().replace('-', '').replace('-', '')
  console.log(todayDate)
  routeID = $("#route-input").val()
  queryURL = "http://127.0.0.1:50032/" + routeID
  console.log(queryURL)

  $.ajax({
    url: queryURL,
    type: "GET",
    beforeSend: function (xhr) {
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.setRequestHeader('X-Content-Type-Options', 'nosniff');
    },
    success: function (rawstops) {
      stops = rawstops._items
      console.log(stops)

      baseRadius = 84;

      for (var j = 9; j >= 0; j--) {
        for (var i = 0; i < stops.length; i++) {
          diff_time = stops[i]["wt_dif_"+j]
          L.circle([parseFloat(stops[i].lat), parseFloat(stops[i].lon)], {
            radius: baseRadius * j,
            stroke: true,
            weight: 0.2,
            color: "#000000",
            fillOpacity: 1,
            fillColor: returnColor(diff_time, colorRamp, colorCode)
          }).addTo(map);

        }
      }
    }
  });

});

var colorRamp=[-Infinity, -80, -46, -13, 0, 43 , Infinity]
var colorCode=["#0080FF", "#5CAEA2" , "#B9DC45", "#FFDC00", "#FF9700", "#FF2000"]

function returnColor(value, colorRamp, colorCode){
  for (var i = 1; i<colorRamp.length; i++){
    if (value >= colorRamp[i-1] && value < colorRamp[i]){
      return colorCode[i-1]
    }
    else{
      continue;
    }
  }
  console.log(value)
  return 
}


function createCORSRequest(method, url) {
  var xhr = new XMLHttpRequest();
  if ("withCredentials" in xhr) {
    xhr.open(method, url, true);

  } else if (typeof XDomainRequest != "undefined") {
    xhr = new XDomainRequest();
    xhr.open(method, url);

  } else {
    xhr = null;

  }
  return xhr;
}
