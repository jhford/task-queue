'use strict';

var uuid = require('uuid'),
    redis = require('redis'),
    debug = require('debug')('queue'),
    rb = require('./refried_beans');

/* Notes:
 *   - options must be a flat object
 *   - module should not be a full path resolved file
 */
function enqueue(module, timeout, options, callback) {
  var client = redis.createClient();

  if (-1 !== module.indexOf('/')) {
    return callback(new Error('module should not be a path'));
  }

  var optionKeys = Object.keys(options);

  // I don't want to deal with nesting objects quite
  // yet.  This is possible, but for now, let's call
  // this a limitation of the api
  for (var i = 0; i < optionKeys.length; i++) {
    if (typeof(options[optionKeys[i]]) === 'object') {
      return callback(new Error('task options must be flat obj'));
    }
  }

  var optId = uuid.v4(),
      taskId = uuid.v4(),
      task = {
        module: module,
        timeout: timeout
      };

  var transaction = client.multi(),
      expectedResponses = [];

  // Insert the options object only if it is non-empty
  if (optionKeys.length > 0) {
    transaction.hmset(optId, options);
    expectedResponses.push('OK');
    task.options = optId;
  } else {
    task.options = rb.NO_OPTIONS;
  }

  // Insert the actual task
  transaction.hmset(taskId, task);
  expectedResponses.push('OK');

  // Push into the pending task queue
  transaction.lpush(rb.PENDING_QUEUE, taskId);
  expectedResponses.push(null);

  // Run the transaction
  transaction.exec(function(err, replies) {
      // Make sure that API errors propogate
      if (err) return callback(err);
      // We want to make sure that the correct amount of replies happen
      if (expectedResponses.length !== replies.length) {
        return callback(new Error('Redis gave us back too many replies'));
      }
      // ... and also that they are the ones we want
      for (var i = 0; i < expectedResponses.length; i++) {
        if (!expectedResponses[i]) {
          continue;
        } else if (expectedResponses[i] !== replies[i]) {
          errObj = new Error('Unable to insert objects into list');
          return callback(errObj);
        }
      }
      // We don't publish the task ID to ensure that the
      // consumer is using the atomic operations of Redis
      // instead of trying to manual fetch the task
      client.publish(rb.PENDING_QUEUE, rb.TASK_QUEUED);
      // Close out connection
      client.end();
      // Huzzah!
      callback(null, rb.PENDING_QUEUE, taskId);
    });
}

module.exports.enqueue = enqueue;
