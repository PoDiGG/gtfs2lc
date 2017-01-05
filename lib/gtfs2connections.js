var csv = require('fast-csv'),
    ConnectionRules = require('./stoptimes/st2c.js'),
    ConnectionsBuilder = require('./ConnectionsBuilder.js'),
    RoutesBuilder = require('./RoutesBuilder.js'),
    Services = require('./services/calendar.js'),
    DateInterval = require('./DateInterval.js'),
    Store = require('./stores/Store.js'),
    through2 = require('through2'),
    moment = require('moment'),
    fs = require('fs'),
    stream = require('stream');

var Mapper = function (options) {
  this._options = options;
  this._options.interval = new DateInterval(options.startDate, options.endDate);
  if (!this._options.store) {
    this._options.store = 'MemStore';
  }
  this._enrich = (options || {}).enrich;
  this._tripInstances = (options || {}).tripInstances;
};

/**
 * Returns a resultStream for connections
 * Step 1: Convert calendar_dates.txt and calendar.txt to service ids mapped to a long list of dates
 * Step 2: Pipe these services towards a leveldb: we want to use them later.
 * Step 3: also index trips.txt in a leveldb on key trip_id
 * Step 4: create a stream of connection rules from stop_times.txt
 * Step 5: pipe this stream to something that expands everything into connections and returns this stream.
 * Caveat: coding this with numerous callbacks and streams, makes this code not chronologically ordered.
 */ 
Mapper.prototype.resultStream = function (path, done) {
  var trips = fs.createReadStream(path + '/trips.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true}));
  trips.on('error', function(e){ console.error('Error reading trips.txt: ' + e); });
  if (this._enrich) {
    var stops = fs.createReadStream(path + '/stops.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true}));
    stops.on('error', function(e){ console.error('Error reading stops.txt: ' + e); });
  }
  var calendarDates = fs.createReadStream(path + '/calendar_dates.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true}));
  calendarDates.on('error', function(e){ console.error('Error reading calendar_dates.txt: ' + e); });
  var delays = null;
  if (fs.existsSync(path + '/delays.txt')) {
    delays = fs.createReadStream(path + '/delays.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true}));
    delays.on('error', function(e){ console.error('Error reading delays.txt: ' + e); });
  }
  var services = fs.createReadStream(path + '/calendar.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true})).pipe(new Services(calendarDates, this._options));
  services.on('error', function(e){ console.error('Error reading calendar.txt: ' + e); });
  //Preparations for step 4
  var connectionRules = fs.createReadStream(path + '/stop_times.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true})).pipe(new ConnectionRules());
  connectionRules.on('error', function(e){ console.error('Error reading stop_times.txt: ' + e); });

  //Step 2 & 3: store in leveldb in 2 hidden directories, or in memory, depending on the options
  var tripsdb = Store(path + '/.trips', this._options.store);
  if (this._enrich) {
    var stopsdb = Store(path + '/.stops', this._options.store);
  }
  var servicesdb = Store(path + '/.services',this._options.store);
  var delaysdb;
  if (delays) {
    delaysdb = Store(path + '/.delays', this._options.store);
  }
  var count = 0;
  var self = this;
  var finished = function () {
    count ++;
    //wait for the (2 or 3) streams to finish (services and trips) to write to the stores
    if (count === (2 + !!self._enrich + !!delays)) {
      console.error("Indexing services and trips succesful!");
      //Step 4 and 5: let's create our connections and routes!
      var combinedStream = new stream.Transform({ objectMode: true });
      combinedStream._transform = function(d, encoding, done) { this.push(d); done() }; // Simply forward data
      var connectionsStream = connectionRules.pipe(new ConnectionsBuilder(tripsdb, servicesdb, stopsdb, delaysdb, self._options));
      connectionsStream.pipe(combinedStream, { end: false });
      connectionsStream.on('end', function () {
        if (self._enrich) {
          var routes = fs.createReadStream(path + '/routes.txt', {encoding:'utf8', objectMode: true}).pipe(csv({objectMode:true,headers: true}));
          routes.on('error', function(e){ console.error('Error reading routes.txt: ' + e); });
          routes.pipe(new RoutesBuilder(self._options)).pipe(combinedStream);
        } else {
          connectionsStream.close();
        }
      });
      done(combinedStream);
    }
  };
  
  services.pipe(through2.obj(function (service, encoding, doneService) {
    if (service['service_id']) {
      servicesdb.put(service['service_id'], service['dates'], doneService);
    }
  })).on('finish', finished);

  if (this._enrich) {
    stops.pipe(through2.obj(function (stop, encoding, doneStop) {
      if (stop['stop_id']) {
        stopsdb.put(stop['stop_id'], stop, doneStop);
      }
    })).on('finish', finished);
  }

  trips.pipe(through2.obj(function (trip, encoding, doneTrip) {
    if (trip['trip_id']) {
      tripsdb.put(trip['trip_id'], trip, doneTrip);
    }
  })).on('finish', finished);

  if (delays) {
    delays.pipe(through2.obj(function (delay, encoding, doneDelay) {
      if (delay['trip_id']) {
        delaysdb.put(delay['trip_id'] + '-' + delay['stop_sequence'], delay, doneDelay);
      }
    })).on('finish', finished);
  }
  
};

module.exports = Mapper;
