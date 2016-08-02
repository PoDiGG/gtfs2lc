/**
 * Pieter Colpaert © Ghent University - iMinds
 * Combines connection rules, trips and services to an unsorted stream of connections
 */
var Transform = require('stream').Transform,
    util = require('util'),
    moment = require('moment'),
    request = require('request');

var ConnectionsBuilder = function (tripsdb, servicesdb, stopsdb, options) {
  Transform.call(this, {objectMode : true});
  this._tripsdb = tripsdb;
  this._servicesdb = servicesdb;
  this._stopsdb = stopsdb;
  this._enrich = (options || {}).enrich;
};

util.inherits(ConnectionsBuilder, Transform);

ConnectionsBuilder.prototype._transform = function (connectionRule, encoding, done) {
  if (this._enrich && !this._stations) {
    var self = this;
    var url = 'http://irail.be/stations/NMBS'; // TODO: don't hardcode?
    request({
      url: url,
      json: true
    }, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        self._stations = {};
        body['@graph'].forEach(function(element) {
          var id = parseInt(element['@id'].replace(url + '/', ''));
          self._stations[id] = {
            country: element['country'],
            latitude: element['latitude'],
            longitude: element['longitude'],
            name: element['name']
          };
        });
        self._actualTransform(connectionRule, encoding, done);
      }
    });
  } else {
    this._actualTransform(connectionRule, encoding, done);
  }
};

ConnectionsBuilder.prototype._actualTransform = function (connectionRule, encoding, done) {
  //Examples of
  // * a connectionRule: {"trip_id":"STBA","arrival_dfm":"6:20:00","departure_dfm":"6:00:00","departure_stop":"STAGECOACH","arrival_stop":"BEATTY_AIRPORT","departure_stop_headsign":"","arrival_stop_headsign":"","pickup_type":""}
  // * a trip: { route_id: 'AAMV',service_id: 'WE',trip_id: 'AAMV4',trip_headsign: 'to Airport',direction_id: '1', block_id: '', shape_id: '' }
  var departureDFM = moment.duration(connectionRule['departure_dfm']);
  var arrivalDFM = moment.duration(connectionRule['arrival_dfm']);
  var arrivalStopId = connectionRule['arrival_stop'];
  var departureStopId = connectionRule['departure_stop'];
  var self = this;
  this._tripsdb.get(connectionRule['trip_id'], function (error, trip) {
    if (!error) {
      self._servicesdb.get(trip['service_id'], function (error, service) {
        if (!error) {
          for (var i in service) {
            var serviceDay = service[i];
            var departureTime = moment(serviceDay, 'YYYYMMDD').add(departureDFM);
            var arrivalTime = moment(serviceDay, 'YYYYMMDD').add(arrivalDFM);
            var data = {
              departureTime: departureTime,
              arrivalTime: arrivalTime,
              arrivalStop: connectionRule['arrival_stop'],
              departureStop: connectionRule['departure_stop'],
              trip: connectionRule['trip_id'],
              route: trip['route_id']
            };
            if (self._enrich) {
              self._stopsdb.get(arrivalStopId, function (error, arrivalStop) {
                self._stopsdb.get(departureStopId, function (error, departureStop) {
                  self._stopsdb.get(arrivalStop.parent_station, function (error, arrivalStation) {
                    self._stopsdb.get(departureStop.parent_station, function (error, departureStation) {
                      arrivalStop.station = self._enrichStation(arrivalStation);
                      departureStop.station = self._enrichStation(departureStation);
                      data.arrivalStopLink = arrivalStop;
                      data.departureStopLink = departureStop;
                      self._getLinkedGeoData(arrivalStop.station.station_lon, arrivalStop.station.station_lat, function(uri) {
                        if (uri) {
                          arrivalStop.station.geodata = uri;
                        }
                        self.push(data);
                      });
                    });
                  });
                });
              });
            } else {
              self.push(data);
            }
          }

        }
        done();
      });
    }
  });
};

ConnectionsBuilder.prototype._enrichStation = function (station) {
  var stationData = this._stations[station.stop_id];
  station.station_name = stationData.name;
  station.country = stationData.country;
  station.station_lat = stationData.latitude;
  station.station_lon = stationData.longitude;
  return station;
};

ConnectionsBuilder.prototype._getLinkedGeoData = function (lon, lat, cb) {
  var url = 'http://linkedgeodata.org/sparql?default-graph-uri=http%3A%2F%2Flinkedgeodata.org&query='; // TODO: don't hardcode?
  var radius = '0.002';
  var query =
      'PREFIX lgdr:<http://linkedgeodata.org/triplify/>' +
      'PREFIX lgdo:<http://linkedgeodata.org/ontology/>' +
      'SELECT ?node {' +
      '    ?node <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat .' +
      '    ?node <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?lon .' +
      '    ?node a <http://linkedgeodata.org/ontology/RailwayStation>' +
      '    FILTER ( ?lat > ' + lat + ' - ' + radius + ' && ?lat < ' + lat + ' + ' + radius + ' && ?lon > ' + lon + ' - ' + radius + ' && ?lon < ' + lon + ' + ' + radius + ' )' +
      '}' +
      'LIMIT 1';
  request({
    url: url + encodeURIComponent(query),
    json: true
  }, function (error, response, body) {
    if (!error && response.statusCode === 200) {
      var bindings = body.results.bindings;
      if (bindings.length > 0) {
        cb(bindings[0].node.value);
      } else {
        cb();
      }
    } else {
      cb();
    }
  });
};

module.exports = ConnectionsBuilder;
