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


var baseLayer = L.esri.basemapLayer('Gray')
map = L.map("map", {
  zoom: 12.5,
  zoomSnap: 0.25,
  center: [40.011829189152486, -82.91261469998747],
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


$(document).ready(function () {
  $('#date-input').val(("2018-02-01"))
  $('#date-pr-input').val(("2018-02-01"))
})

map.on("dragend", function (e) {
  console.log(e)
})

map.on("click", function (e) {
  // console.log(e)
})

L.control.scale({ position: "bottomleft" }).addTo(map);
var north = L.control({ position: "topright" });
north.onAdd = function (map) {
  var div = L.DomUtil.create("div", "info");
  div.id = 'north_arrow'
  div.innerHTML = '<img style="height:120px;width:auto;" src="img/north_arrow.png">';
  return div;
}
north.addTo(map);



$(function () {
  $("form").submit(function () { return false; });
});

var tran;

function zoomIn(e) {
  if (event.key === 'Enter') {
    var zoomLevel = parseFloat(e.value)
    console.log(zoomLevel)
    map.setZoom(zoomLevel);

  }
}

L.control.browserPrint({
  printModes: ["Portrait", "Landscape", "Auto", "Custom"],
  position: "topright"
}).addTo(map);


$("#snap-btn").click(function () {
  $("#status").html("Running...")

});

$("#down-btn").click(function () {

});


$("#start-btn").click(function () {
  var todayDate = $("#date-input").val().replace('-', '').replace('-', '')
  console.log(todayDate)
  var tripID = $("#trip-input").val()
  var queryURL = "http://127.0.0.1:32154/R" + todayDate + '?where={"trip_id":"' + tripID + '"}'
  console.log(queryURL)

  $.ajax({
    url: queryURL,
    type: "GET",
    beforeSend: function (xhr) {
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.setRequestHeader('X-Content-Type-Options', 'nosniff');
    },
    success: function (rawstops) {
      var stops = rawstops._items
      console.log(stops)

      var baseRadius = 84;

      var colorRamp = [0, 30, 60, 120, 150, 270, 360, Infinity] // red is waiting more time; blue is saving more time
      var colorCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#9932CC"]

      for (var i = 0; i < stops.length; i++) {
        diff_time = stops[i]["time"] - stops[i]["scheduled_time"]
        var cir = L.circle([parseFloat(stops[i].lat), parseFloat(stops[i].lon)], {
          radius: baseRadius * 3,
          stroke: true,
          weight: 0.2,
          color: "#000000",
          fillOpacity: 1,
          info: stops[i],
          fillColor: returnColor(diff_time, colorRamp, colorCode)
        });

        cir.on("mouseover", function (d) {
          var popup = L.popup()
            .setLatLng([parseFloat(d.target.options.info.lat), parseFloat(d.target.options.info.lon)])
            .setContent("<span>Stop sequence: " + d.target.options.info["stop_sequence"] + "</span></br><span>Delay: " + (d.target.options.info["time"] - d.target.options.info["scheduled_time"]) + "s</span>")
            .openOn(map);
          console.log(d.target.options.info["stop_sequence"])
        })
        cir.addTo(map);

      }

      var legend = L.control({ position: "bottomright" });
      legend.onAdd = function (map) {
        var div = L.DomUtil.create("div", "info legend");
        div.id = 'legend'

        var legendContent2 = "<span style='font-size:30;'>Legend</span>"
        var title = "Delay (seconds)"
        legendContent2 += "<h3>" + title + "</h3>"
        legendContent2 += '<table><tbody>'
        for (var i = 0; i < colorCode.length; i++) {
          console.log(i)
          if (colorRamp[i] == -Infinity) {
            labelContent2 = "( -∞, " + colorRamp[i + 1] + ")";
          }
          else {
            if (colorRamp[i + 1] == Infinity) {
              labelContent2 = "[" + colorRamp[i] + ", ∞ )";
            }
            else {
              labelContent2 = "[" + colorRamp[i] + ", " + colorRamp[i + 1] + ")";
            }
          }
          legendContent2 += "<tr valign='middle'>" +
            "<td class='tablehead' align='middle'>" + getColorBlockString(colorCode[i]) + "</td>" +
            "<td class='tablecontent' align='right' style='width:180px;'><span style='width:90%;font-size:30;font:'>" + labelContent2 + "</span><td>" + "</tr>";
        }
        legendContent2 += "</tbody><table>";

        div.innerHTML = legendContent2;
        return div;
      }
      legend.addTo(map);
    }
  });

});


$("#start-4-btn").click(function () {
  var todayDate = $("#date-4-input").val().replace('-', '').replace('-', '')
  console.log(todayDate)
  var tripID = $("#trip-4-input").val()
  var queryURL = "http://127.0.0.1:8080/-2_delay_reclamation"
  console.log(queryURL)

  $.ajax({
    url: queryURL,
    type: "GET",
    beforeSend: function (xhr) {
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.setRequestHeader('X-Content-Type-Options', 'nosniff');
    },
    success: function (rawstops) {
      var stops = rawstops._items
      console.log(stops)
      var colorRamp = [0, 5, 8, 10, 12, 15, 25, Infinity] // red is waiting more time; blue is saving more time
      var colorCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#9932CC"]

      for (var i = 0; i < stops.length; i++) {
        diff_time = stops[i].delay_reclamation_count / stops[i].total_count * 100

        L.circle([parseFloat(stops[i].stop_lat), parseFloat(stops[i].stop_lon)], {
          radius: 200,
          stroke: true,
          weight: 0.2,
          color: "#000000",
          fillOpacity: 1,
          fillColor: returnColor(diff_time, colorRamp, colorCode)
        }).addTo(map);
      }

      var title = "Delay relacamation rate (%)"
      var legend = L.control({ position: "bottomright" });
      legend.onAdd = function (map) {
        var div = L.DomUtil.create("div", "info legend");
        div.id = 'legend'

        var legendContent2 = "<span style='font-size:30;'>Legend</span>"
        legendContent2 += "<h3>" + title + "</h3>"
        legendContent2 += '<table><tbody>'
        for (var i = 0; i < colorCode.length; i++) {
          console.log(i)
          if (colorRamp[i] == -Infinity) {
            labelContent2 = "( -∞, " + colorRamp[i + 1] + ")";
          }
          else {
            if (colorRamp[i + 1] == Infinity) {
              labelContent2 = "[" + colorRamp[i] + ", ∞ )";
            }
            else {
              labelContent2 = "[" + colorRamp[i] + ", " + colorRamp[i + 1] + ")";
            }
          }
          legendContent2 += "<tr valign='middle'>" +
            "<td class='tablehead' align='middle'>" + getColorBlockString(colorCode[i]) + "</td>" +
            "<td class='tablecontent' align='right' style='width:180px;'><span style='width:90%;font-size:30;font:'>" + labelContent2 + "</span><td>" + "</tr>";
        }
        legendContent2 += "</tbody><table>";

        div.innerHTML = legendContent2;
        return div;
      }
      legend.addTo(map);
    }
  });

});

$("#start-3-btn").click(function () {
  var routeID = $("#route-3-input").val()
  var variableCode = $("#variable-3-input").val()
  var queryURL = "http://127.0.0.1:50033/APC_" + routeID
  // var queryURL = "http://127.0.0.1:50033/delay"
  console.log(queryURL)

  $.ajax({
    url: queryURL,
    type: "GET",
    beforeSend: function (xhr) {
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.setRequestHeader('X-Content-Type-Options', 'nosniff');
    },
    success: function (rawstops) {
      var stops = rawstops._items
      console.log(stops)
      visualizationReduce(stops, variableCode);
    }
  });
});

function visualizationReduce(stops, variableCode) {
  var baseRadius = 84;
  var colorRamp, colorCode;

  // var title = "ST - PT Optimal Waiting Time Difference (min)"
  // var title = "PT Optimal Missed Risk (%)"
  // var title = "PT Optimal Buffers (seconds)"
  var title = "AT Waiting Time (min)"

  // var colorRamp = [-Infinity, -60, -30, 0, 30, 60, 120, Infinity] // nr and pr_opt diff
  var colorRamp = [0, 150, 200, 250, 300, 450, 600, Infinity] // PR opt waiting time per se
  // var colorRamp = [0, 140, 150, 160, 170, 180, 200, Infinity] // buffer
  // var colorRamp = [0, 2.5, 5, 10, 15, 20, 25, 100] // miss rate
  // var colorRamp = [0, 100, 150, 200, 250, 300, 600, Infinity] // ar and pr_opt diff
  // var colorRamp = [-Infinity, 0, 200, 300, 400, 500, 600, Infinity] // rr and pr_opt diff
  // var colorRamp = [0, 250, 300, 350, 400, 500, 600, Infinity] // er and pr_opt diff

  // var colorRamp = [0, 100, 200, 300, 400, 500, 600, Infinity] // ar and er and nr


  // var colorRamp = [0, 10, 25, 40, 50, 60, 75, 100] // rr miss rate


  var colorCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#9932CC"]

  for (var j = 9; j >= 0; j--) {
    for (var i = 0; i < stops.length; i++) {
      var cir = L.circle([parseFloat(stops[i].lat), parseFloat(stops[i].lon)], {
        radius: baseRadius * j,
        stroke: true,
        weight: 0.2,
        color: "#000000",
        fillOpacity: 1,
        info: stops[i],
        stop_id: stops[i]["stop_id"],
        stop_sequence: stops[i]["stop_sequence"],
        delay: stops[i]["wt_nr"],
        j: j,
        value: stops[i][variableCode + "_" + j.toString()],
        miss_rate: stops[i]["mc_pr_opt_" + j.toString()] / stops[i]["total"] * 100,
        // fillColor: returnColor(stops[i]["delay"] / stops[i]["count"], colorRamp, colorCode) // Delay

        fillColor: returnColor(stops[i][variableCode], colorRamp, colorCode) // NR, AR, ER
        // fillColor: returnColor(stops[i][variableCode + "_" + j.toString()], colorRamp, colorCode) // PR_opt, RR and buffer
        // fillColor: returnColor(stops[i][variableCode + "_" + j.toString()] - stops[i]["wt_nr"] , colorRamp, colorCode) // PR_opt, RR difference
        // fillColor: returnColor(stops[i][variableCode + "_" + j.toString()] / stops[i]["total"] * 100, colorRamp, colorCode) // miss rate
        // fillColor: returnColor(stops[i][variableCode] / stops[i]["total"]*100, colorRamp, colorCode) // miss rate for static
        // fillColor: returnColor(stops[i]["wt_er"] - stops[i][variableCode + "_" + j.toString()] , colorRamp, colorCode) // ar/er and pr_opt diff
        // fillColor: returnColor((stops[i][variableCode + "_" + j.toString()])/ stops[i]["total"]*100, colorRamp, colorCode) // rr and pr_opt diff, for missrate or waiting time

        // fillColor: returnColor(stops[i]["wt_ar"] - stops[i]["wt_er"] , colorRamp, colorCode) // ar/er and pr_opt diff

      });
      cir.on("click", function (d) {
        // console.log(d.target.options.info["delay"] / d.target.options.info["count"])

        console.log(d.target.options.stop_id, d.target.options.stop_sequence, d.target.options.j, d.target.options.value, d.target.options.miss_rate)
      })

      cir.addTo(map);
      console.log((stops[i]["mc_rr_" + j.toString()] - stops[i][variableCode + "_" + j.toString()]) / stops[i]["total"])
    }
  }


  var legend = L.control({ position: "bottomright" });
  legend.onAdd = function (map) {
    var div = L.DomUtil.create("div", "info legend");
    div.id = 'legend'

    var legendContent2 = "<span style='font-size:30;'>Legend</span>"
    legendContent2 += "<h3>" + title + "</h3>"
    legendContent2 += '<table><tbody>'
    for (var i = 0; i < colorCode.length; i++) {
      console.log(i)
      if (colorRamp[i] == -Infinity) {
        labelContent2 = "( -∞, " + colorRamp[i + 1] + ")";
      }
      else {
        if (colorRamp[i + 1] == Infinity) {
          labelContent2 = "[" + colorRamp[i] + ", ∞ )";
        }
        else {
          labelContent2 = "[" + colorRamp[i] + ", " + colorRamp[i + 1] + ")";
        }
      }
      legendContent2 += "<tr valign='middle'>" +
        "<td class='tablehead' align='middle'>" + getColorBlockString(colorCode[i]) + "</td>" +
        "<td class='tablecontent' align='right' style='width:180px;'><span style='width:90%;font-size:30;font:'>" + labelContent2 + "</span><td>" + "</tr>";
    }
    legendContent2 += "</tbody><table>";

    div.innerHTML = legendContent2;
    return div;
  }
  legend.addTo(map);
}

function getColorBlockString(color) {
  var div = '<div class="legendbox" style="padding:0px;background:' + color + '"></div>'
  return div;
}

function visualization(stops, variableCode) {
  var baseRadius = 84;
  var colorRamp, colorCode;

  eval("colorRamp = " + variableCode + "Ramp");
  eval("colorCode = " + variableCode + "Code");
  for (var j = 9; j >= 0; j--) {
    for (var i = 0; i < stops.length; i++) {
      ///
      var diff_time = dataTransformation(variableCode, stops, i, j);
      console.log(diff_time);
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

function dataTransformation(variableCode, stops, i, j) {
  switch (variableCode) {
    case "ave_buff":
      diff_time = stops[i]["sum_buff" + "_" + j] / stops[i]["trip_count"]
      break;
    default:
      diff_time = stops[i][variableCode + "_" + j]
  }

  return diff_time;
}


var ave_buffRamp = [0, 10, 20, 30, 45, 60, 90, Infinity]
var ave_buffCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#000000"]

var max_buffRamp = [90, 120, 150, 180, 210, 270, 300, Infinity]
var max_buffCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#000000"]

var miss_riskRamp = [0, 1, 3, 5, 7, 15, 30, Infinity]
var miss_riskCode = ["#0080FF", "#5CAEA2", "#B9DC45", "#FFDC00", "#FF9700", "#FF2000", "#000000"]

function returnColor(value, colorRamp, colorCode) {
  for (var i = 1; i < colorRamp.length; i++) {
    if (value >= colorRamp[i - 1] && value < colorRamp[i]) {
      return colorCode[i - 1]
    }
    else {
      continue;
    }
  }
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
