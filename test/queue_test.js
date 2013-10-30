'use strict';

var redis = require('redis'),
    debug = require('debug')('queueTest'),
    rb = require('../lib/refried_beans'),
    queue = require('../lib/queue.js'),
    uuid = require('uuid');

suite('enqueue', function() {
  // This suite is missing tests for the API returning
  // errors :/

  var r; // For our redis client obj

  setup(function() {
    r = redis.createClient();
    r.flushall();
  });

  teardown(function() {
    r.end();
  });

  test('inserts one item in list', function(done) {

    function lengthCheck(err, data) {
      assert.ok(data === 1, 'inserts one item');
      done(err);
    }

    queue.enqueue(
      'passing_task.js',
      2000,
      {},
      function(err, queueId, taskId) {
        if (err) done(err);
        r.llen(queueId, lengthCheck);
      });
  });

  test('inserts two items in list', function(done) {

    function lengthCheck(err, data) {
      assert.ok(data === 2, 'inserts two items');
      done(err);
    }

    function firstEnqueue(err, queueId, taskId) {
      if (err) done(err);
      queue.enqueue(
        'passing_task.js',
        2000,
        {},
        secondEnqueue);
    }

    function secondEnqueue(err, queueId, taskId) {
      if (err) done(err);
      r.llen(queueId, lengthCheck);
    }

    queue.enqueue(
      'passing_task.js',
      2000,
      {},
      firstEnqueue);
  });

  test('tracks the correct uuid', function(done) {

    function taskIdCheck(err, queueId, taskId) {
      if (err) done(err);
      r.lrange(queueId, 0, 0, function(err, data) {
        assert.equal(taskId, data, 'id of task is correct');
        done(err);
      });
    }

    queue.enqueue(
      'passing_task.js',
      2000,
      {},
      taskIdCheck);
  });

  test('stores correct data', function(done) {

    var options = {
          num: 1.5,
          string: 'Hello world'
      },
      timeout = 2000,
      module = 'passing_task.js';

    queue.enqueue(
      'passing_task.js',
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

  test('will publish message', function(done) {
    r.subscribe(rb.PENDING_QUEUE);

    function onMessage(channel, message) {
      assert.equal(channel, rb.PENDING_QUEUE);
      assert.equal(message, rb.TASK_QUEUED);
      r.end();
      done();
    }

    queue.enqueue(
      'passing_task.js',
      2000,
      {},
      function(err, queueId, taskId) {
        if (err) done(err);
        r.on('message', onMessage);
      });
  });

  test('store task without options', function(done) {
    queue.enqueue(
      'passing_task.js',
      2000,
      {},
      function(err, queueId, taskId) {
        if (err) done(err);
        r.hgetall(taskId, function(err, obj) {
          assert.equal(obj.options, rb.NO_OPTIONS);
          done(err);
        });
      });
  });

  test('invalid module name', function(done) {
    queue.enqueue(
      '/path/that/is/absolute.js',
      2000,
      {},
      function(err, queueId, taskId) {
        var expErrMsg = 'should not be a path';
        assert.ok(err);
        assert.ok(-1 !== err.message.indexOf(expErrMsg));
        done();
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

    queue.enqueue(
      'passing_task.js',
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

suite('materialize tasks', function() {
  var r;

  setup(function() {
    r = redis.createClient();
    r.flushall();
  });

  teardown(function() {
    r.end();
  });

  var id = uuid.v4(),
      optId = uuid.v4(),
      module = 'module.js',
      timeout = 2000,
      options = { num: 1, string: 'test-string' };

  suite('without options', function() {
    var testObj;

    setup(function(done) {
      testObj = {
        module: module,
        timeout: timeout,
        options: rb.NO_OPTIONS
      };
      r.hmset(id, testObj, function(err, res) {
        assert.ok(res === 'OK');
        done(err);
      });
    });

    test('task with no options', function(done) {
      queue.materializeTask(id, function(err, task) {
        assert.ok(testObj.module, task.module);
        assert.ok(testObj.timeout, task.timeout);
        assert.ok({}, task.options);
        done(err);
      });
    });

    test('task with invalid options', function(done) {
      r.hdel(id, 'options', function(err, res) {
        assert.equal(res, 1);
        if (err) done(err);
        queue.materializeTask(id, function(err, task) {
          assert.ok(err);
          assert.ok(!task);
          done();
        });
      });
    });
  });

  suite('options', function() {
    var testObj;
    var testOpt;

    setup(function(done) {
      testObj = {
        module: module,
        timeout: timeout,
        options: optId
      };
      testOpt = options;

      r.hmset(id, testObj, function(err, res) {
        assert.ok(res === 'OK');
        r.hmset(optId, options, function(err, res) {
          assert.ok(res === 'OK');
          done(err);
        });
      });
    });

    test('task with real options', function(done) {
      queue.materializeTask(id, function(err, task) {
        assert.ifError(err);
        assert.equal(task.module, module);
        assert.equal(task.timeout, timeout);
        assert.deepEqual(task.options, options);
        done(err);
      });
    });

  });


});

suite('claiming', function() {
  var r; // For our redis client obj
  var task = {
    module: 'testing.js',
    timeout: 2000,
    options: {
      num: 1,
      string: 'string'
    }
  };

  setup(function(done) {
    r = redis.createClient();

    function enqueueOne(err) {
      r.flushall();
      queue.enqueue(task.module, task.timeout, task.options, function(err) {
        assert.ifError(err);
        enqueueTwo();
      });
    }

    function enqueueTwo(err) {
      queue.enqueue(task.module, task.timeout, task.options, function(err) {
        done(err);
      });
    }

    queue.enqueue(task.module, task.timeout, task.options, enqueueOne);
  });

  teardown(function() {
    r.end();
  });

  test('claiming object removes from pending', function(done) {
    queue.claim(function(err) {
      assert.ifError(err);
      r.llen(rb.PENDING_QUEUE, function(err, length) {
        assert.equal(1, length);
        done(err);
      });
    });
  });

  test('claiming object inserts to in progress', function(done) {
    queue.claim(function(err) {
      assert.ifError(err);
      r.llen(rb.INPROGRESS_QUEUE, function(err, length) {
        assert.equal(1, length);
        done(err);
      });
    });
  });

  test('claiming returns the correct object', function(done) {
    queue.claim(function(err, claimedTask) {
      assert.deepEqual(task, claimedTask);
      done(err);
    });
  });

 });

