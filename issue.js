const Rabbitmq = require("./index.js")

const rabbitmq = new Rabbitmq();

rabbitmq.config("amqp://localhost");

rabbitmq.on("error", console.error)

function listen() {
	rabbitmq.listen("issue", (err, msg) => {
		if (err) {
			console.log("try to restart", err.toString())
			return setTimeout(listen, 5000)
		}
		setTimeout(() => {
			throw "eww"
		}, 1000)
		console.log("received", msg.payload);
		msg.release();
	});
}

function sendSim() {
	console.log("sending")
	rabbitmq.send({ "value": Math.random() }, { "queue": "issue" }, (err) => {
		console.log("sended", err)
		setTimeout(sendSim, 10000);
	})
}

listen();

sendSim();