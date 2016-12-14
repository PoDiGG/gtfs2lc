'use strict';

var level = require('level');

var LevelStore = function (name) {
  this.name = name;
  this._store = level(name);
};

LevelStore.prototype.get = function (key, cb) {
  this._store.get(key, function (error, object) {
    if (!error) {
      cb(null, JSON.parse(object));
    } else {
      cb(error);
    }
  });
};

LevelStore.prototype.getPromise = function (key) {
  var self = this;
  return new Promise((resolve, reject) => {
    self._store.get(key, function (error, object) {
      if (!error) {
        resolve(JSON.parse(object));
      } else {
        reject(error);
      }
    });
  });
};

LevelStore.prototype.put = function (key, value, cb) {
  this._store.put(key, value, {valueEncoding: 'json'}, cb);
};

module.exports = LevelStore;
