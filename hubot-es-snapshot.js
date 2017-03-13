/*
  Description:
    Hubot elasticsearch snapshot script

  Configuration:

  Commands:
    hubot run snapshot server repository
    hubot schedule snapshot server repository "cron"
    hubot delete snapshot server repository
    hubot list snapshots
*/

var env = require('nconf').argv().env().file('default', 'config.json');
var room = 'engineering-git';
var CronJob = require('cron').CronJob;
var Promise = require('bluebird');
var snapshotJobs = {};

module.exports = function(hubot) {

  setTimeout(function() {
    initializeSnapshotJobs(hubot);
  }, 10000);

  hubot.respond(/run snapshot (.+) (.+)$/i, function(message){
    var ref = message.match.slice(1);
    var server = ref[0];
    var repository = ref[1];
    createSnapshot(
      server,
      repository,
      env.get('aws:access_key'),
      env.get('aws:secret_key'),
      {
        send:function(msg) {
          sendMessage(hubot, message, msg);
        }
      }
    );
  });

  hubot.respond(/schedule snapshot (.+) (.+) [“”"'‘](.+)[“”"'’]$/i, function(message){
    var ref = message.match.slice(1);
    var server = ref[0];
    var repository = ref[1];
    var cronTime = ref[2];
    sendMessage(hubot, message, "Scheduling " + server + " " + repository + " " + '"' + cronTime + '"');
    scheduleSnapshot(
      hubot,
      server,
      repository,
      cronTime)
      .then(function(){
        sendMessage(hubot, message, "Scheduled " + server + " " + repository + " " + '"' + cronTime + '"');
      })
  });

  hubot.respond(/delete snapshot (.+) (.+)/i, function(message) {
    var ref = message.match.slice(1);
    var server = ref[0];
    var repository = ref[1];
    deleteSnapshot(hubot, server, repository)
      .then(function(){
        sendMessage(hubot, message, "Deleted scheduled snapshot " + server + " " + repository);
      },
      function(err){
        sendMessage(hubot, message, "Failed deletion of snapshot " + server + " " + repository);
      })
  });

  hubot.respond(/list snapshots/i, function(message) {
    var snapshots = hubot.brain.get('snapshots');
    var list = Object.keys(snapshots||{}).reduce(function(o,d) {
      o += '['+d+']: '+snapshots[d][2]+'\n';
      return o;
    }, '');
    sendMessage(hubot,message, list.length && list || 'No scheduled snapshots');
  });

};

function sendMessage(hubot, message, msg) {
  hubot.messageRoom(room, msg);
  if (message && message.envelope && message.envelope.room !== room)
    message.send(msg);
}

function registerSnapshot(hubot, server, repository, cronTime, res){
  var key = jobKey(server,repository);
  if (snapshotJobs[key])
    snapshotJobs[key].stop();

  var job = new CronJob({
    cronTime: cronTime,
    onTick: function(){
      return createSnapshot(
        server,
        repository,
        env.get('aws:access_key'),
        env.get('aws:secret_key'),
        res
      );
    },
    start: true,
    timeZone: 'America/New_York'
  });

  snapshotJobs[key] = job;
}

function jobKey(server, repository){
  return server +","+ repository;
}

function initializeSnapshotJobs(hubot){
  var snapshots = hubot.brain.get('snapshots') || {};
  for (k in snapshots){
    var job = snapshots[k];
    registerSnapshot(
      hubot,
      job[0],
      job[1],
      job[2],
      {
        send:function(msg) {
          hubot.messageRoom(room, msg);
        }
      }
    );
  }
}

function saveSnapshot(hubot, server, repository, cronTime){
  var snapshots = hubot.brain.get('snapshots') || {};
  var key = jobKey(server,repository);
  snapshots[key] = [server, repository, cronTime];
  hubot.brain.set('snapshots', snapshots);
  return Promise.resolve(hubot.brain.save());
}

function deleteSnapshot(hubot, server, repository){
  var snapshots = hubot.brain.get('snapshots') || {};
  var key = jobKey(server,repository);
  delete snapshots[key];
  hubot.brain.set('snapshots', snapshots);
  return Promise.resolve(hubot.brain.save())
    .then(function(){
      snapshotJobs[key].stop();
      delete snapshotJobs[key]
    });
}

function cleanSnapshots(server, repository, daysToKeep, accessKey, secretKey, res){
  var elasticsearch = require('elasticsearch'),
    moment = require('moment-timezone');

  var esClient = new elasticsearch.Client({
    hosts: server,
    amazonES: {
      region: 'us-east-1',
      accessKey: accessKey,
      secretKey: secretKey
    },
    connectionClass: require('http-aws-es'),
    keepAlive : true
  });

  return esClient.cat.snapshots({format: 'json', repository: repository})
  .then(function(d){
    var toRemove = d.snapshots
      .filter(function(r){
        moment(r.start_time_in_millis) < moment().subtract(daysToKeep, "days")
      });
    if (d.snapshots.length >= toRemove.length + daysToKeep)
      return toRemove;
  })
  .then(function(d){
    d.map(function(r){
      esClient.snapshot.delete({
        repository: repository,
        snapshot: r.snapshot,
        masterTimeout: '15s'
      })
      .then(console.log, console.log)
    })
  })
}

function scheduleSnapshot(hubot, server, repository, cronTime){
  return saveSnapshot(hubot, server, repository, cronTime)
    .then(function(){
      registerSnapshot(
        hubot,
        server,
        repository,
        cronTime,
        {
          send: function(msg){
            hubot.messageRoom(room, msg);
          }
        }
      );
    });
}


function createSnapshot(host, repository, accessKey, secretKey, res){
  var checkInterval = 30*60*1000;
  var masterTimeout = "30s";

  var elasticsearch = require('elasticsearch'),
    moment = require('moment-timezone');

  var esClient = new elasticsearch.Client({
    hosts: host,
    amazonES: {
      region: 'us-east-1',
      accessKey: accessKey,
      secretKey: secretKey
    },
    connectionClass: require('http-aws-es'),
    keepAlive : true
  });

  var snapshot = moment().tz('America/New_York').format('YYYYMMDDHHmm');


  function check(){
    function checkFn(){
      return esClient.snapshot.get({repository: repository, snapshot: snapshot})
        .then(function(d) {
          if (d && d.snapshots && d.snapshots.length === 1 && d.snapshots[0].state !== "IN_PROGRESS")
            res.send(d.snapshots[0].state+' Snapshot '+host+' on repository '+repository);
          else if (d.snapshots[0].state === "IN_PROGRESS")
            check();
        });
    }
    setTimeout(checkFn,checkInterval);
  }

  function afterCreate(){
    return esClient.snapshot.get({repository: repository, snapshot: snapshot})
      .then(function(d) {
        return d.snapshots[0].state;
      });
  }

  function catchAfterCreate(err){
    return esClient.snapshot.get({repository: repository, snapshot: snapshot})
      .then(function(d) {
        if (d && d.snapshots && d.snapshots.length === 1)
          return d.snapshots[0].state;
        else
          throw err;
      });
  }

  res.send('Snapshotting '+host+' on repository '+repository);
  return esClient.snapshot.create({masterTimeout: masterTimeout, waitForCompletion: false, repository: repository, snapshot: snapshot})
    .then(afterCreate,catchAfterCreate)
    .then(function(state) {
      if (state === "IN_PROGRESS")
        check();
      else
        res.send('Failed Snapshot '+host+' on repository '+repository);
      return {host: host, snapshot: snapshot, repository: repository};
    });
}
