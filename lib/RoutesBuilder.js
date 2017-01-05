'use strict';

/**
 * Ruben Taelman © Ghent University - iMinds
 * Prepares routes for serialization
 */
var Transform = require('stream').Transform,
    util = require('util');

var RoutesBuilder = function (options) {
  Transform.call(this, {objectMode : true});
};

util.inherits(RoutesBuilder, Transform);

RoutesBuilder.prototype._transform = function (routeRule, encoding, done) {
  routeRule.type = 'route';
  this.push(routeRule);
  done();
};

module.exports = RoutesBuilder;
