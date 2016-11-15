/**
 * Pieter Colpaert © Ghent University - iMinds 
 * Combines connection rules, trips and services to an unsorted stream of connections
 */
var Transform = require('stream').Transform,
    util = require('util'),
    moment = require('moment');

var Connections2JSONLD = function (baseUris, context) {
  Transform.call(this, {objectMode : true});
  this.context = context || {
    "@context" : {
      "lc" : "http://semweb.mmlab.be/ns/linkedconnections#",
      "Connection" : "http://semweb.mmlab.be/ns/linkedconnections#Connection",
      "Stop" : "http://vocab.gtfs.org/terms#Stop",
      "Station": "http://vocab.gtfs.org/terms#Station",
      "gtfs" : "http://vocab.gtfs.org/terms#",
      "departureStop" : {
        "@type" : "@id",
        "@id" : "http://semweb.mmlab.be/ns/linkedconnections#departureStop"
      },
      "arrivalStop" : {
        "@type" : "@id",
        "@id" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalStop"
      },
      "departureTime" : "http://semweb.mmlab.be/ns/linkedconnections#departureTime",
      "arrivalTime" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalTime",
      "lat" : "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
      "long" : "http://www.w3.org/2003/01/geo/wgs84_pos#long",
      "platformCode" : "http://vocab.gtfs.org/terms#platformCode",
      "code" : "http://vocab.gtfs.org/terms#code",
      "parentStation" : "http://vocab.gtfs.org/terms#parentStation",
      "label": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "country": "http://dbpedia.org/ontology/country",
      "sameAs": "http://www.w3.org/2002/07/owl#sameAs",
      "delayValue": "http://semweb.mmlab.be/ns/linkedconnectionsmeta#delayValue",
      "delayReason": "http://semweb.mmlab.be/ns/linkedconnectionsmeta#delayReason"
    } 
  };
  
  var defaultBaseUris = {
    stops : 'http://example.org/stops/',
    connections : 'http://example.org/connections/',
    trips : 'http://example.org/trips/',
    routes : 'http://example.org/routes/',
    delays : 'http://example.org/delays/'
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
  this._delayCount = 0;
};

util.inherits(Connections2JSONLD, Transform);

Connections2JSONLD.prototype._transform = function (connection, encoding, done) {
  var id = this._baseUris.connections + encodeURIComponent(connection.departureTime + connection.departureStop + connection.trip);
  var departureStopId = this._baseUris.stops + encodeURIComponent(connection.departureStop);
  var arrivalStopId = this._baseUris.stops + encodeURIComponent(connection.arrivalStop);
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
  done(null, {
    "@id" : id,
    "@type" : "Connection",
    "departureStop" : departureStopId,
    "arrivalStop" : arrivalStopId,
    "departureTime" : connection.departureTime.toISOString(),
    "arrivalTime" : connection.arrivalTime.toISOString(),
    "gtfs:trip" : this._baseUris.trips + encodeURIComponent(connection.trip),
    "gtfs:route" : this._baseUris.routes + encodeURIComponent(connection.route)
  });
};

Connections2JSONLD.prototype._pushStop = function (stopLink, stopId) {
  var stationId = this._baseUris.stops + encodeURIComponent(stopLink.parent_station);
  var stationLink = stopLink.station;
  this.push({
    "@id" : stopId,
    "@type": 'Stop',
    "lat": "\"" + stopLink.stop_lat + "\"",
    "long": "\"" + stopLink.stop_lon + "\"",
    "platformCode": "\"" + stopLink.platform_code + "\"",
    "code": "\"" + stopLink.stop_id + "\"",
    "parentStation": "\"" + stopLink.stationId + "\""
  });

  if (stationLink) {
    var stationData = {
      "@id" : stationId,
      "@type": 'Station',
      "lat": "\"" + stationLink.stop_lat + "\"",
      "long": "\"" + stationLink.stop_lon + "\"",
      "label": "\"" + stationLink.station_name + "\"",
      "country": stationLink.country
    };
    if (stationLink.geodata) {
      stationData["sameAs"] = stationLink.geodata;
    }
    this.push(stationData);
  }
};

Connections2JSONLD.prototype._pushDelay = function (connectionId, delay, delay_reason, type) {
  var delayId = this._baseUris.delays + this._delayCount++;
  this.push({
    "@id": delayId,
    "@type": 'http://semweb.mmlab.be/ns/linkedconnectionsmeta#Delay',
    "delayValue": "\"" + delay + "\"",
    "delayReason": delay_reason
  });
};

module.exports = Connections2JSONLD;
