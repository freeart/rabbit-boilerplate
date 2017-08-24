const Rabbitmq = require("./index.js"),
    async = require("async");

const rabbitmq = new Rabbitmq();

rabbitmq.config("amqp://localhost");

rabbitmq.on("error", console.error)

function sendSim() {
    console.log("sending")
    rabbitmq.send({"value": Math.random()}, "queuename2").then(() => {
        console.log("sended")
        setTimeout(sendSim, 10000);
    }).catch(console.error)
}

function listen() {
    rabbitmq.listen("queuename2", (err, msg) => {
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