var socket = io();
var NEGATIVE = 1;
var POSITIVE = 1;
var NEUTRAL = 1;
var layout = { title: 'Tweets Sentiment for Trump' };
socket.on('channel', function(msg) {
    if (msg.status == "NEGATIVE") {
        NEGATIVE = msg.count;
    } else if (msg.status == "POSITIVE") {
        POSITIVE = msg.count;
    } else {
        NEUTRAL = msg.count;
    }

    let pie_init_data = [{
        values: [NEGATIVE, POSITIVE, NEUTRAL],
        labels: ['NEGATIVE', 'POSITIVE', 'NEUTRAL'],
        type: 'pie'
    }];

    Plotly.newPlot('pie_chart', pie_init_data);
});

/*
var data = [
  {
    x: ['giraffes', 'orangutans', 'monkeys'],
    y: [20, 14, 23],
    type: 'bar'
  }
];

Plotly.newPlot('myDiv', data);
*/