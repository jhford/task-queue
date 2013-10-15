'use strict';

var redis = require('redis'),
    debug = require('debug')('queueTest'),
    rb = require('../lib/refried_beans');

suite('enqueue', function() {

  var subject = require('../lib/queue.js');

  var r;

  setup(function() {
    r = redis.createClient();
    r.flushall();
  });

  test('inserts one item in list', function(done) {
    subject.enqueue(
      require.resolve('./passing_task.js'),
      2000,
      {},
      function(err, queueId, taskId) {
        if (err) done(err);
        var r = redis.createClient();
        r.llen(queueId, function(err, data) {
          if (err) done(err);
          assert.ok(data === 1, 'inserts one item');
          done(err);
        });
      });
  });

  test('tracks the correct uuid', function(done) {
    subject.enqueue(
      require.resolve('./passing_task.js'),
      2000,
      {},
      function(err, queueId, taskId) {
        if (err) done(err);
        var r = redis.createClient();
        r.lrange(queueId, 0, 0, function(err, data) {
          assert.equal(taskId, data, 'id of task is correct');
          done(err);
        });
      });
  });

  test('stores correct data', function(done) {

    var options = {
          num: 1.5,
          string: 'Hello world'
      },
      timeout = 2000,
      module = require.resolve('./passing_task.js');

    subject.enqueue(
      require.resolve('./passing_task.js'),
      timeout,
      options,
      function(err, queueId, taskId) {
        if (err) done(err);
        r.hgetall(taskId, function(err, obj) {
          assert.ok(obj.module === module, 'stores correct module');
          assert.ok(parseInt(obj.timeout, 10) === timeout,
                    'stores correct timeout');
          r.hgetall(obj.options, function(err, obj) {
            assert.deepEqual(obj, options, 'stores options correctly');
            done(err);
          });
        });
      });
  });

  test('cannot store nested objects', function(done) {

    var options = {
          num: 1.5,
          string: 'Hello world',
          obj: { num2: 2 }
      },
      timeout = 2000,
      module = require.resolve('./passing_task.js');

    subject.enqueue(
      require.resolve('./passing_task.js'),
      timeout,
      options,
      function(err, queueId, taskId) {
        assert.ok(err, 'should generate error');
        assert.ok(-1 !== err.message.indexOf('flat'),
                  'error message should be about flat objs');
        done();
      });
  });

});