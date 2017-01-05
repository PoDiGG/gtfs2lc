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
  this._pushedTrips = {};
  this._previousConnectionTrip = null;
  this._previousConnectionData = null;
  this._connectionCount = 0;
};

util.inherits(ConnectionsBuilder, Transform);

ConnectionsBuilder.prototype._delayStringToPeriod = function (delayString) {
  var delay = parseInt(delayString);
  if (!delay) return false;
  return durational.toString(durational.fromSeconds(Math.ceil(delay / 1000)));
};

ConnectionsBuilder.prototype._transform = function (connectionRule, encoding, done) {
  //Examples of
  // * a connectionRule: {"trip_id":"STBA","arrival_dfm":"6:20:00","departure_dfm":"6:00:00","departure_stop":"STAGECOACH","arrival_stop":"BEATTY_AIRPORT","departure_stop_headsign":"","arrival_stop_headsign":"","pickup_type":""}
  // * a trip: { route_id: 'AAMV',service_id: 'WE',trip_id: 'AAMV4',trip_headsign: 'to Airport',direction_id: '1', block_id: '', shape_id: '' }
  var departureDFM = moment.duration(connectionRule['departure_dfm']);
  var arrivalDFM = moment.duration(connectionRule['arrival_dfm']);

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

        // Finally, add delays if there is a delays db active
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
      }));
    })
    .then(() => { done() })
    .catch((e) => { console.error(e); done() });
};

module.exports = ConnectionsBuilder;
