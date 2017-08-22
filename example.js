const Rabbitmq = require("./index.js"),
    async = require("async");

const rabbitmq = new Rabbitmq();

rabbitmq.config("amqp://localhost");

function sendSim() {
    async.retry({times: 3, interval: 5000}, (cb) => {
        rabbitmq.send({"value": Math.random()}, "queuename", cb);
    }, (err) => {
        console.log("sended", err && err.toString());
        setTimeout(sendSim, 10000);
    })
}

function listen() {
    rabbitmq.listen("queuename", (err, msg) => {
        if (err) {
            console.log("try to restart", err.toString())
            return setTimeout(listen, 5000)
        }
        console.log("received", msg.payload);
        msg.release();
    });
}

sendSim();
listen();