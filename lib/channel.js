var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Promise = require('./promise');

/**
 * Channel constructor.
 *
 * @param {Connection} connection
 * @param {String} [name] optional channel/collection name, default is 'mubsub'
 * @param {Object} [options] optional options
 *   - `size` max size of the collection in bytes, default is 5mb
 *   - `max` max amount of documents in the collection
 *   - `retryInterval` time in ms to wait if no docs found, default is 200ms
 *   - `recreate` recreate the tailable cursor on error, default is true
 * @api public
 */
function Channel(connection, name, options) {
    options || (options = {});
    options.capped = true;
    // In mongo v <= 2.2 index for _id is not done by default
    options.autoIndexId = true;
    options.size || (options.size = 1024 * 1024 * 5);
    options.retryInterval || (options.retryInterval = 200);
    options.recreate != null || (options.recreate = true);

    this.options = options;
    this.connection = connection;
    this.closed = false;
    this.listening = null;
    this.name = name || 'mubsub';

    this.setMaxListeners(0);
    this.create().listen();
}

module.exports = Channel;
util.inherits(Channel, EventEmitter);

/**
 * Close the channel.
 *
 * @return {Channel} this
 * @api public
 */
Channel.prototype.close = function () {
    this.closed = true;

    return this;
};

/**
 * Publish an event.
 *
 * @param {String} event
 * @param {Object} [arguments]
 * @return {Channel} this
 * @api public
 */
Channel.prototype.publish = function (event) {
    var self = this, args = Array.prototype.slice.call(arguments);
    this.ready(function (collection) {
        collection.insert({ event: event, args: args }, function (err, docs) {
            if (err) return self.emit('error', err);
        });
    });

    return this;
};

/**
 * Subscribe an event.
 *
 * @param {String} [event] if no event passed - all events are subscribed.
 * @param {Function} callback
 * @return {Channel} this
 * @api public
 */
Channel.prototype.subscribe = function (event, callback) {
    var self = this;

    if (typeof event == 'function') {
        callback = event;
        event = 'message';
    }

    this.on(event, callback);
    this.emit('subscribe', event);

    return this;
};

/**
 * Unsubscribe an event.
 *
 * @param {String} [event] if no event passed - all events are unsubscribed.
 * @param {Function} callback
 * @return {Channel} this
 * @api public
 */
Channel.prototype.unsubscribe = function (event) {
    var self = this;

    if (event === void 0) {
        this.removeAllListeners();
    }
    else {
        this.removeAllListeners(event);
    }
    this.emit('unsubscribe', event);

    return this;
};

/**
 * Create a channel collection.
 *
 * @return {Channel} this
 * @api private
 */
Channel.prototype.create = function () {
    var self = this;

    function create() {
        self.connection.db.createCollection(
            self.name,
            self.options,
            self.collection.resolve.bind(self.collection)
        );
    }

    this.collection = new Promise();
    this.connection.db ? create() : this.connection.once('connect', create);

    return this;
};

/**
 * Create a listener which will emit events for subscribers.
 * It will listen to any document with event property.
 *
 * @param {Object} [latest] latest document to start listening from
 * @return {Channel} this
 * @api private
 */
Channel.prototype.listen = function (latest) {
    var self = this;

    this.latest(latest, this.handle(true, function (latest, collection) {
        var cursor = collection
                .find(
                    { _id: { $gt: latest._id }},
                    { tailable: true, numberOfRetries: -1, tailableRetryInterval: self.options.retryInterval }
                )
                .sort({ $natural: 1 });

        var next = self.handle(function (doc) {
            // There is no document only if the cursor is closed by accident.
            // F.e. if collection was dropped or connection died.
            if (!doc) {
                return setTimeout(function () {
                    self.emit('error', new Error('Mubsub: broken cursor.'));
                    if (self.options.recreate) {
                        self.create().listen(latest);
                    }
                }, 1000);
            }

            latest = doc;

            if (doc.event && doc.event === doc.args[0]) {
                self.emit.apply(self, doc.args);
                doc.args[0] = 'message';
                self.emit.apply(self, doc.args);
            }
            self.emit('document', doc);
            process.nextTick(more);
        });

        var more = function () {
            cursor.nextObject(next);
        };

        more();
        self.listening = collection;
        self.emit('ready', collection);
    }));

    return this;
};

/**
 * Get the latest document from the collection. Insert a dummy object in case
 * the collection is empty, because otherwise we don't get a tailable cursor
 * and need to poll in a loop.
 *
 * @param {Object} [latest] latest known document
 * @param {Function} callback
 * @return {Channel} this
 * @api private
 */
Channel.prototype.latest = function (latest, callback) {
    var self = this;

    this.collection.then(function (err, collection) {
        if (err) return callback(err);

        collection
            .find(latest ? { _id: latest._id } : null)
            .sort({ $natural: -1 })
            .limit(1)
            .nextObject(function (err, doc) {
                if (err || doc) return callback(err, doc, collection);

                collection.insert({ dummy: true }, { safe: true }, function (err, docs) {
                    callback(err, docs[0], collection);
                });
            });
    });

    return this;
};

/**
 * Return a function which will handle errors and consider channel and connection
 * state.
 *
 * @param {Boolean} [exit] if error happens and exit is true, callback will not be called
 * @param {Function} callback
 * @return {Function}
 * @api private
 */
Channel.prototype.handle = function (exit, callback) {
    var self = this;

    if (typeof exit === 'function') {
        callback = exit;
        exit = null;
    }

    return function () {
        if (self.closed || self.connection.destroyed) return;

        var args = [].slice.call(arguments);
        var err = args.shift();

        if (err) self.emit('error', err);
        if (err && exit) return;

        callback.apply(self, args);
    };
};

/**
 * Call back if collection is ready for publishing.
 *
 * @param {Function} callback
 * @return {Channel} this
 * @api private
 */
Channel.prototype.ready = function (callback) {
    if (this.listening) {
        callback(this.listening);
    } else {
        this.once('ready', callback);
    }

    return this;
};
