'use strict';

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
  this._previousConnectionTrip = null;
  this._previousConnectionData = null;
  this._connectionCount = 0;
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

  // Output to show the progress
  process.stderr.write("\rConnections: +" + ++this._connectionCount);

  var self = this;
  var trip, service;
  this._tripsdb.getPromise(connectionRule['trip_id'])
    .catch(err => { console.error(err); })
    .then((r_trip) => {
      trip = r_trip;
      return self._servicesdb.getPromise(trip['service_id'])
    })
    .then((r_service) => {
      service = r_service;
      return Promise.all(service.map((serviceDay) => {
        // Base connection information
        var departureTime = moment(serviceDay, 'YYYYMMDD').add(departureDFM);
        var arrivalTime = moment(serviceDay, 'YYYYMMDD').add(arrivalDFM);
        var data = {
          type: 'connection',
          departureTime: departureTime,
          arrivalTime: arrivalTime,
          arrivalStop: connectionRule['arrival_stop'],
          departureStop: connectionRule['departure_stop'],
          trip: connectionRule['trip_id'],
          route: trip['route_id'],
        };

        // Optionally add information about previous connection
        if (self._enrich) {
          if (self._previousConnectionTrip === trip['trip_id']) {
            data.next_of = self._previousConnectionData;
          }
          self._previousConnectionData = data;
          self._previousConnectionTrip = trip['trip_id'];
        }

        // Emit trip information only once
        if (self._tripInstances && !self._pushedTrips[trip['trip_id']]) {
          self._pushedTrips[trip['trip_id']] = true;
          data.trip_data = {
            trip: trip['trip_id'],
            service: trip['service_id'],
            route: trip['route_id']
          };
        }

        // Optionally add station information for start and end stop of this connection
        var prepromise = Promise.all([]);
        if (self._enrich) {
          var arrivalStop, departureStop;
          prepromise = Promise.all([
            // Enrich arrival stop
            self._stopsdb.getPromise(arrivalStopId)
              .then((r_arrivalStop) => {
                arrivalStop = r_arrivalStop;
                return self._stopsdb.getPromise(arrivalStop.parent_station || arrivalStop.stop_id)
              })
              .then((arrivalStation) => {
                return new Promise((resolve, reject) => {
                  // Don't emit the same stop twice
                  if (!self._pushedStops[arrivalStop.stop_id]) {
                    self._pushedStops[arrivalStop.stop_id] = true;
                    data.arrivalStopLink = arrivalStop;
                    self._enrichStation(arrivalStation, function (station) {
                      arrivalStop.station = station;
                      resolve();
                    });
                  } else {
                    resolve();
                  }
                });
              })
              .catch(() => Promise.resolve()),
            // Enrich departure stop
            self._stopsdb.getPromise(departureStopId)
              .then((r_departureStop) => {
                departureStop = r_departureStop;
                return self._stopsdb.getPromise(departureStop.parent_station || departureStop.stop_id)
              })
              .then((departureStation) => {
                return new Promise((resolve, reject) => {
                  // Don't emit the same stop twice
                  if (!self._pushedStops[departureStop.stop_id]) {
                    self._pushedStops[departureStop.stop_id] = true;
                    data.departureStopLink = departureStop;
                    self._enrichStation(departureStation, function (station) {
                      departureStop.station = station;
                      resolve();
                    });
                  } else {
                    resolve();
                  }
                });
              })
              .catch(() => Promise.resolve()),
          ]);
        }

        // Finally, add delays if there is a delays db active
        return prepromise.then(() => {
          if (self._delaysdb) {
            return self._delaysdb.getPromise(connectionRule['trip_id'] + '-' + connectionRule['stop_sequence'])
              .then((delay) => {
                if (delay) {
                  data.delay_departure = self._delayStringToPeriod(delay.delay_departure);
                  data.delay_arrival = self._delayStringToPeriod(delay.delay_arrival);
                  data.delay_departure_reason = delay.delay_departure_reason;
                  data.delay_arrival_reason = delay.delay_arrival_reason;
                }
                self.push(data);
                return Promise.resolve();
              })
              .catch(() => {
                self.push(data);
                return Promise.resolve();
              });
          } else {
            self.push(data);
            return Promise.resolve();
          }
        });
      }));
    })
    .then(() => { done() })
    .catch((e) => { console.error(e); done() });
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
