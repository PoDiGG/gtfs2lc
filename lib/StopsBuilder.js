'use strict';

/**
 * Ruben Taelman © Ghent University - iMinds
 * Prepares stops for serialization
 */
var Transform = require('stream').Transform,
    request = require('request'),
    util = require('util');

var StopsBuilder = function (stopsdb, options) {
  Transform.call(this, {objectMode : true});
  this._stopsdb = stopsdb;
  this._pushedStations = {};
  this._stationInstances = (options || {}).stationInstances;
  this._stopsCount = 0;
};

util.inherits(StopsBuilder, Transform);

StopsBuilder.prototype._transform = function (stopRule, encoding, done) {
  if (!this._stations) {
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
        self._actualTransform(stopRule, encoding, done);
      }
    });
  } else {
    this._actualTransform(stopRule, encoding, done);
  }
};

StopsBuilder.prototype._actualTransform = function (stopRule, encoding, done) {
  // Output to show the progress
  process.stderr.write("\rStops: +" + ++this._stopsCount);

  stopRule.type = 'stop';
  var self = this;
  this._stopsdb.getPromise(stopRule.parent_station)
    .catch(() => stopRule)
    .then((station) => {
      return new Promise((resolve, reject) => {
        self._enrichStation(station, function (station) {
          stopRule.station = station;
          resolve();
        });
      })})
    .then(() => {
      this.push(stopRule);
      done();
    });
};

StopsBuilder.prototype._enrichStation = function (station, cb) {
  if (!this._pushedStations[station.stop_id]) {
    this._pushedStations[station.stop_id] = true;
    var stationData = this._stations[station.stop_id];
    if (stationData) {
      station.station_name = stationData.name;
      station.country = stationData.country;
      station.station_lat = stationData.latitude;
      station.station_lon = stationData.longitude;

      /*this._getLinkedGeoData(station.station_lon, station.station_lat, function (uri) {
        if (uri) {
          station.geodata = uri;
        }
        cb(station);
      });*/
      // We don't need this slow enrichment step at the moment
      cb(station);
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

StopsBuilder.prototype._getLinkedGeoData = function (lon, lat, cb) {
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

module.exports = StopsBuilder;
