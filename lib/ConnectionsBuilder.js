/**
 * Pieter Colpaert © Ghent University - iMinds
 * Combines connection rules, trips and services to an unsorted stream of connections
 */
var Transform = require('stream').Transform,
    util = require('util'),
    moment = require('moment'),
    request = require('request'),
    durational = require('durational');

var ConnectionsBuilder = function (tripsdb, servicesdb, stopsdb, delaysdb, options) {
  Transform.call(this, {objectMode : true});
  this._tripsdb = tripsdb;
  this._servicesdb = servicesdb;
  this._stopsdb = stopsdb;
  this._delaysdb = delaysdb;
  this._enrich = (options || {}).enrich;
  this._tripInstances = (options || {}).tripInstances;
  this._stationInstances = (options || {}).stationInstances;
  this._pushedStops = {};
  this._pushedStations = {};
  this._pushedTrips = {};
  this._pushedRoutes = {};
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

ConnectionsBuilder.prototype._delayStringToPeriod = function (delayString) {
  var delay = parseInt(delayString);
  if (!delay) return false;
  return durational.toString(durational.fromSeconds(Math.ceil(delay / 1000)));
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
          var count = 0;
          var doneProxy = function(data) {
            if (self._delaysdb) {
              self._delaysdb.get(connectionRule['trip_id'] + '-' + connectionRule['stop_sequence'], function (error, delay) {
                if (delay) {
                  data.delay_departure = self._delayStringToPeriod(delay.delay_departure);
                  data.delay_arrival = self._delayStringToPeriod(delay.delay_arrival);
                  data.delay_departure_reason = delay.delay_departure_reason;
                  data.delay_arrival_reason = delay.delay_arrival_reason;
                }
                self.push(data);
                if (++count == service.length) done();
              });
            } else {
              self.push(data);
              if (++count == service.length) done();
            }
          };
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
            if (self._tripInstances && !self._pushedTrips[trip['trip_id']]) {
              self._pushedTrips[trip['trip_id']] = true;
              data.trip_data = {
                trip: trip['trip_id'],
                service: trip['service_id'],
                route: trip['route_id']
              };
              if (!self._pushedRoutes[trip['route_id']]) {
                self._pushedRoutes[trip['route_id']] = true;
                data.trip_data.emitRoute = true;
              }
            }
            if (self._enrich) {
              self._stopsdb.get(arrivalStopId, function (error, arrivalStop) {
                self._stopsdb.get(departureStopId, function (error, departureStop) {
                  self._stopsdb.get(arrivalStop.parent_station || arrivalStop.stop_id, function (error, arrivalStation) {
                    self._stopsdb.get(departureStop.parent_station || departureStop.stop_id, function (error, departureStation) {
                      var stationCount = 0;
                      var doneStation = function() {
                        if (++stationCount == 2) {
                          doneProxy(data);
                        }
                      };

                      if (!self._pushedStops[arrivalStop.stop_id]) {
                        self._pushedStops[arrivalStop.stop_id] = true;
                        data.arrivalStopLink = arrivalStop;
                        self._enrichStation(arrivalStation, function (station) {
                          arrivalStop.station = station;
                          doneStation();
                        });
                      } else {
                        doneStation();
                      }

                      if (!self._pushedStops[departureStop.stop_id]) {
                        self._pushedStops[departureStop.stop_id] = true;
                        data.departureStopLink = departureStop;
                        self._enrichStation(departureStation, function (station) {
                          departureStop.station = station;
                          doneStation();
                        });
                      } else {
                        doneStation();
                      }
                    });
                  });
                });
              });
            } else {
              doneProxy(data);
            }
          }

        } else {
          done();
        }
      });
    }
  });
};

ConnectionsBuilder.prototype._enrichStation = function (station, cb) {
  if (!this._pushedStations[station.stop_id]) {
    this._pushedStations[station.stop_id] = true;
    var stationData = this._stations[station.stop_id];
    if (stationData) {
      station.station_name = stationData.name;
      station.country = stationData.country;
      station.station_lat = stationData.latitude;
      station.station_lon = stationData.longitude;

      this._getLinkedGeoData(station.station_lon, station.station_lat, function (uri) {
        if (uri) {
          station.geodata = uri;
        }
        cb(station);
      });
    } else if (this._stationInstances) {
      cb({
        station_name: station.stop_name,
        station_lat: station.stop_lat,
        station_lon: station.stop_lon
      });
    } else {
      cb();
    }
  } else {
    cb();
  }
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
