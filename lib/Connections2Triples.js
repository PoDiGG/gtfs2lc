/**
 * Pieter Colpaert © Ghent University - iMinds 
 * Combines connection rules, trips and services to an unsorted stream of connections
 */
var Transform = require('stream').Transform,
    util = require('util'),
    moment = require('moment');

var Connections2Triples = function (baseUris) {
  Transform.call(this, {objectMode : true});
  var defaultBaseUris = {
    stops : 'http://example.org/stops/',
    connections : 'http://example.org/connections/',
    trips : 'http://example.org/trips/',
    routes : 'http://example.org/routes/',
    delays : 'http://example.org/delays/',
    services : 'http://example.org/services/',
    stations : 'http://example.org/stations/'
  };
  if (!baseUris) {
    baseUris = defaultBaseUris;
  } else {
    if (typeof baseUris.stops !== 'string') {
      baseUris.stops = defaultBaseUris.stops;
    }
    if (typeof baseUris.trips !== 'string') {
      baseUris.trips = defaultBaseUris.trips;
    }
    if (typeof baseUris.routes !== 'string') {
      baseUris.routes = defaultBaseUris.routes;
    }
    if (typeof baseUris.connections !== 'string') {
      baseUris.connections = defaultBaseUris.connections;
    }
    if (typeof baseUris.services !== 'string') {
      baseUris.services = defaultBaseUris.services;
    }
    if (typeof baseUris.stations !== 'string') {
      baseUris.stations = defaultBaseUris.stations;
    }
  }
  this._baseUris = baseUris;
  this._delayCount = 0;
};

util.inherits(Connections2Triples, Transform);

Connections2Triples.prototype._getConnectionId = function(connection) {
  return this._baseUris.connections + encodeURIComponent(connection.departureTime + connection.departureStop + connection.trip);
};

Connections2Triples.prototype._transform = function (data, encoding, done) {
  switch (data.type) {
    case 'connection':
      this._transformConnection(data, encoding, done);
      break;
    case 'route':
      this._transformRoute(data, encoding, done);
      break;
    default:
      throw new Error('Invalid data element type found to transform: ' + data);
      break;
  }
};

Connections2Triples.prototype._transformConnection = function (connection, encoding, done) {
  var id = this._getConnectionId(connection);
  var departureStopId = this._baseUris.stops + encodeURIComponent(connection.departureStop);
  var arrivalStopId = this._baseUris.stops + encodeURIComponent(connection.arrivalStop);
  var tripid = this._baseUris.trips + encodeURIComponent(connection.trip);
  var routeId = this._baseUris.routes + encodeURIComponent(connection.route);
  if (connection.next_of) {
    this.push({
      subject : this._getConnectionId(connection.next_of),
      predicate :'http://semweb.mmlab.be/ns/linkedconnections#nextConnection',
      object : id
    });
  }
  this.push({
    subject : id,
    predicate :'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    object : 'http://semweb.mmlab.be/ns/linkedconnections#Connection'
  });
  this.push({
    subject : id,
    predicate :'http://semweb.mmlab.be/ns/linkedconnections#departureStop',
    object : departureStopId
  });
  this.push({
    subject : id,
    predicate :'http://semweb.mmlab.be/ns/linkedconnections#arrivalStop',
    object : arrivalStopId
  });
  this.push({
    subject : id,
    predicate :'http://semweb.mmlab.be/ns/linkedconnections#departureTime',
    object : '"' + connection.departureTime.toISOString() + '"^^http://www.w3.org/2001/XMLSchema#dateTime'
  });
  this.push({
    subject : id,
    predicate :'http://semweb.mmlab.be/ns/linkedconnections#arrivalTime',
    object : '"' + connection.arrivalTime.toISOString() + '"^^http://www.w3.org/2001/XMLSchema#dateTime'
  });
  this.push({
    subject : id,
    predicate :'http://vocab.gtfs.org/terms#trip',
    object : tripid
  });
  this.push({
    subject : id,
    predicate :'http://vocab.gtfs.org/terms#route',
    object : routeId
  });

  if (connection.departureStopLink) {
    this._pushStop(connection.departureStopLink, departureStopId);
  }
  if (connection.arrivalStopLink) {
    this._pushStop(connection.arrivalStopLink, arrivalStopId);
  }
  if (connection.delay_departure) {
    this._pushDelay(id, connection.delay_departure, connection.delay_departure_reason, 'departure');
  }
  if (connection.delay_arrival) {
    this._pushDelay(id, connection.delay_arrival, connection.delay_arrival_reason, 'arrival');
  }
  if (connection.trip_data) {
    this.push({
      subject : tripid,
      predicate :'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
      object : 'http://vocab.gtfs.org/terms#Trip'
    });
    this.push({
      subject : tripid,
      predicate : 'http://vocab.gtfs.org/terms#route',
      object : routeId
    });
  }

  done();
};

Connections2Triples.prototype._pushStop = function (stopLink, stopId) {
  var stationId = this._baseUris.stations + encodeURIComponent(stopLink.parent_station || stopLink.stop_id);
  var stationLink = stopLink.station;
  this.push({
    subject : stopId,
    predicate : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    object : 'http://vocab.gtfs.org/terms#Stop'
  });
  this.push({
    subject : stopId,
    predicate : 'http://www.w3.org/2003/01/geo/wgs84_pos#lat',
    object : "\"" + stopLink.stop_lat + "\""
  });
  this.push({
    subject : stopId,
    predicate : 'http://www.w3.org/2003/01/geo/wgs84_pos#long',
    object : "\"" + stopLink.stop_lon + "\""
  });
  this.push({
    subject : stopId,
    predicate : 'http://vocab.gtfs.org/terms#platformCode',
    object : "\"" + (stopLink.platform_code || 0) + "\""
  });
  this.push({
    subject : stopId,
    predicate : 'http://vocab.gtfs.org/terms#code',
    object : "\"" + stopLink.stop_id + "\""
  });

  if (stationLink) {
    this.push({
      subject: stopId,
      predicate: 'http://vocab.gtfs.org/terms#parentStation',
      object: stationId
    });
    this.push({
      subject: stationId,
      predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
      object: 'http://vocab.gtfs.org/terms#Station'
    });
    this.push({
      subject: stationId,
      predicate: 'http://www.w3.org/2000/01/rdf-schema#label',
      object: "\"" + stationLink.station_name + "\""
    });
    if (stationLink.country) {
      this.push({
        subject: stationId,
        predicate: 'http://dbpedia.org/ontology/country',
        object: stationLink.country
      });
    }
    this.push({
      subject: stationId,
      predicate: 'http://www.w3.org/2003/01/geo/wgs84_pos#lat',
      object: "\"" + stationLink.station_lat + "\""
    });
    this.push({
      subject: stationId,
      predicate: 'http://www.w3.org/2003/01/geo/wgs84_pos#long',
      object: "\"" + stationLink.station_lon + "\""
    });
    if (stationLink.geodata) {
      this.push({
        subject: stationId,
        predicate: 'http://www.w3.org/2002/07/owl#sameAs',
        object: stationLink.geodata
      });
    }
  }
};

Connections2Triples.prototype._pushDelay = function (connectionId, delay, delay_reason, type) {
  var delayId = this._baseUris.delays + this._delayCount++;
  var reasonId = delayId + "/reason0";
  this.push({
    subject: connectionId,
    predicate :'http://semweb.mmlab.be/ns/linked-connections-delay#' + type + 'Delay',
    object : delayId
  });
  this.push({
    subject: delayId,
    predicate :'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    object: 'http://semweb.mmlab.be/ns/linked-connections-delay#Delay'
  });
  this.push({
    subject: delayId,
    predicate :'http://semweb.mmlab.be/ns/linked-connections-delay#delayValue',
    object: "\"" + delay + "\"^^http://www.w3.org/2001/XMLSchema#duration"
  });
  this.push({
    subject: delayId,
    predicate :'http://semweb.mmlab.be/ns/linked-connections-delay#delayReason',
    object: reasonId
  });
  this.push({
    subject: reasonId,
    predicate :'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    object: delay_reason
  });
};

Connections2Triples.prototype._transformRoute = function (route, encoding, done) {
  var routeId = this._baseUris.routes + encodeURIComponent(route.route_id);
  this.push({
    subject: routeId,
    predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    object: 'http://vocab.gtfs.org/terms#Route'
  });
  done();
};

module.exports = Connections2Triples;
