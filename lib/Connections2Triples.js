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
    routes : 'http://example.org/routes/'
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
  }
  this._baseUris = baseUris;
  this._count = 0;
};

util.inherits(Connections2Triples, Transform);

Connections2Triples.prototype._transform = function (connection, encoding, done) {
  var id = this._baseUris.connections + encodeURIComponent(connection.departureTime + connection.departureStop + connection.trip);
  var departureStopId = this._baseUris.stops + encodeURIComponent(connection.departureStop);
  var arrivalStopId = this._baseUris.stops + encodeURIComponent(connection.arrivalStop);
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
    object : this._baseUris.trips + encodeURIComponent(connection.trip)
  });
  this.push({
    subject : id,
    predicate :'http://vocab.gtfs.org/terms#route',
    object : this._baseUris.routes + encodeURIComponent(connection.route)
  });

  if (connection.departureStopLink) {
    this._pushStop(connection.departureStopLink, departureStopId);
  }
  if (connection.arrivalStopLink) {
    this._pushStop(connection.arrivalStopLink, arrivalStopId);
  }

  done();
};

Connections2Triples.prototype._pushStop = function (stopLink, stopId) {
  var stationId = this._baseUris.stops + encodeURIComponent(stopLink.parent_station);
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
  if (stopLink.parent_station) {
    this.push({
      subject: stopId,
      predicate: 'http://vocab.gtfs.org/terms#parentStation',
      object: stationId
    });
  }

  if (stationLink) {
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
    this.push({
      subject: stationId,
      predicate: 'http://dbpedia.org/ontology/country',
      object: stationLink.country
    });
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

module.exports = Connections2Triples;
