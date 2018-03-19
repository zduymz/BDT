var app = require('express')();
var http = require('http').Server(app);
var server = app.listen(3000);
var io = require('socket.io')(server);
var config = require('./config');

app.get('/', function(req, res) {
    res.sendFile(__dirname + '/index.html');
});

app.get('/js/chart.js', function(req, res) {
    res.sendFile(__dirname + '/chart.js');
});

function callSockets(io, message) {
    io.sockets.emit('channel', message);
}

var kafka = require('kafka-node');
var options = {
    host: undefined,
    kafkaHost: config.KAFKA_BOOTSTRAP_SERVER,
    zk: undefined,
    batch: undefined,
    ssl: false,
    groupId: undefined,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'latest',
    outOfRangeOffset: 'earliest',
    migrateHLC: false,
    migrateRolling: true
};
var consumer = new kafka.ConsumerGroup(options, config.KAFKA_TOPIC);

consumer.on('message', function(message) {
    /* TEST DRAWING CHART BY RANDOM VALUES
    var v1 = Math.floor(Math.random() * 50) + 1 ;
    var v2 = Math.floor(Math.random() * 50) + 1 ;
    var v3 = Math.floor(Math.random() * 50) + 1  ;
    var msg = {'v': [v1, v2, v3]};
    callSockets(io, msg)
    */

    var data = JSON.parse(message.value);
    callSockets(io, data)
});

