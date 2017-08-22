```javascript
const Rabbitmq = require("./index.js"),  
    async = require("async");

const rabbitmq = new Rabbitmq();

//can be hot-swapped
rabbitmq.config("amqp://localhost");
 
rabbitmq.send({"value": Math.random()}, "queuename", (err) => {
    if (err){
        return console.error(err)
    }
    console.log("sended")
});

rabbitmq.listen("queuename", (err, msg) => {
    if (err) {
        return console.error("you should call rabbitmq.listen again", err.toString())
    }
    console.log("received", msg.payload);
    msg.release(); //or msg.reject();
});

```