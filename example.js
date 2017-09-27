const Rabbitmq = require("./index.js"),
	async = require("async");

const rabbitmq = new Rabbitmq();

rabbitmq.config("amqp://localhost");

rabbitmq.on("error", console.error)

function sendSim() {
	console.log("sending")
	rabbitmq.send({"value": Math.random()}, "queuename").then(() => {
		console.log("sended")
		setTimeout(sendSim, 10000);
	}).catch(console.error)
}

function broadcast() {
	console.log("broadcasting")
	rabbitmq.broadcast({"value": Math.random()}, "broadcastname").then(() => {
		console.log("broadcasted")
		setTimeout(broadcast, 10000);
	}).catch(console.error)
}

function stream() {
	rabbitmq.stream("broadcastname", (err, msg) => {
		if (err) {
			console.log("try to restart", err.toString())
			return setTimeout(stream, 5000)
		}
		console.log("streaming", msg.payload);
	});
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

stream();
setTimeout(() => broadcast(), 1000)
