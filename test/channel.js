var assert = require('assert');
var mubsub = require('../lib/index');
var data = require('./fixtures/data');
var helpers = require('./helpers');

describe('Channel', function () {
    beforeEach(function () {
        this.client = mubsub(helpers.uri);
        this.channel = this.client.channel('channel');
    });

    it('unsubscribes properly', function (done) {
        var self = this;

        this.channel.subscribe('a', function (data) {
            assert.equal(data, 'a');
            self.channel.unsubscribe();
            done();
        });

        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
    });

    it('unsubscribes if channel is closed', function (done) {
        var self = this;

        this.channel.subscribe('a', function (data) {
            assert.equal(data, 'a');
            self.channel.close();
            done();
        });

        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
    });

    it('unsubscribes if client is closed', function (done) {
        var self = this;

        this.channel.subscribe('a', function (data) {
            assert.equal(data, 'a');
            self.client.close();
            done();
        });

        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
        this.channel.publish('a', 'a');
    });

    it('can subscribe and publish different events', function (done) {
        var self = this, triggered = {};

        function complete (ev) {
            assert.equal(triggered[ev], undefined);
            triggered[ev] = true;
            if (Object.keys(triggered).length === 3) {
                self.channel.unsubscribe();
                done();
            }
        }

        this.channel.subscribe('a', function (data) {
            assert.equal(data, 'a');
            complete('a');
        });

        this.channel.subscribe('b', function (data) {
            assert.deepEqual(data, {b: 1});
            complete('b');
        });

        this.channel.subscribe('c', function (data) {
            assert.deepEqual(data, ['c']);
            complete('c');
        });

        this.channel.publish('a', 'a');
        this.channel.publish('b', { b: 1 });
        this.channel.publish('c', ['c']);
    });

    it('can publish multiple arguments like emit', function (done) {
        var self = this;

        this.channel.subscribe('a', function (foo, bar, baz) {
            assert.equal(foo, 'foo');
            assert.equal(bar, 'bar');
            assert.equal(baz, 'baz');
            self.channel.unsubscribe();
            done();
        });

        this.channel.publish('a', 'foo', 'bar', 'baz');
    });

    it('gets lots of subscribed data fast enough', function (done) {
        var channel = this.client.channel('channel.bench', { size: 1024 * 1024 * 100 });

        var n = 5000;
        var count = 0;

        channel.subscribe('a', function (_data) {
            assert.deepEqual(_data, data);

            if (++count == n) {
                channel.unsubscribe();
                done();
            }
        });

        for (var i = 0; i < n; i++) {
            channel.publish('a', data);
        }
    });
});
