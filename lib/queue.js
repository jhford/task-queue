'use strict';

var uuid = require('uuid'),
    redis = require('redis'),
    debug = require('debug')('queue'),
    rb = require('./refried_beans');

/* Notes:
 *   - options must be a flat object
 */
function enqueue(module, timeout, options, callback) {
  var client = redis.createClient();

  var optionKeys = Object.keys(options);

  for (var i = 0; i < optionKeys.length; i++) {
    if (typeof(options[optionKeys[i]]) === 'object') {
      return callback(new Error('task options must be flat obj'));
    }
  }

  var optId = uuid.v4(),
      taskId = uuid.v4(),
      task = {
        module: module,
        timeout: timeout,
        options: optId
      };
  debug('Options ID: %s', optId);
  debug('Task ID: %s', taskId);

  client.multi()
    .hmset(optId, optionKeys.length > 0 ? options : {empty: true})
    .hmset(taskId, task)
    .lpush(rb.PENDING_QUEUE, taskId)
    .exec(function(err, replies) {
      client.end();
      if (err) return callback(err);
      debug(replies);
      if (replies[0] !== 'OK' ||
          replies[1] !== 'OK') {
        callback(new Error('Unable to insert objects into list'));
      }
      callback(null, rb.PENDING_QUEUE, taskId);
    });
}

module.exports.enqueue = enqueue;
