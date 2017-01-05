'use strict';

/**
 * Ruben Taelman © Ghent University - iMinds
 * Prepares routes for serialization
 */
var Transform = require('stream').Transform,
    util = require('util');

var RoutesBuilder = function (options) {
  Transform.call(this, {objectMode : true});
  this._routesCount = 0;
};

util.inherits(RoutesBuilder, Transform);

RoutesBuilder.prototype._transform = function (routeRule, encoding, done) {
  // Output to show the progress
  process.stderr.write("\rRoutes: +" + ++this._routesCount);

  routeRule.type = 'route';
  this.push(routeRule);
  done();
};

module.exports = RoutesBuilder;
