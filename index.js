const amqp = require('amqplib/callback_api'),
    EventEmitter = require('events'),
    domain = require('domain');

class Wrapper extends EventEmitter {
    constructor() {
        super();

        this.connectString = null;
        this.conn = {};
        this.channelPool = {};
        this.callback = {
            listen: {}
        };
    }

    config(connectString) {
        this.connectString = connectString;
    }

    __bail(err, conn, cb) {
        if (conn) {
            conn.close(() => {
                cb && cb(err);
            });
        } else {
            cb && setImmediate(() => cb(err));
        }
    }

    listen(queue, cb, prefetch) {
        this.__connect(queue, (err, conn) => {
            if (err) {
                return this.__bail(err, conn, cb)
            }

            this.__channel(queue, true, (err, ch) => {
                if (err) {
                    return this.__bail(err, conn, cb)
                }

                this.callback.listen[queue] = cb;

                ch.assertQueue(queue, {durable: true});
                ch.prefetch(prefetch || 1);

                ch.consume(queue, (msg) => {
                    if (msg) {
                        let timeoutID = setTimeout(() => {
                            this.__bail("process frozen", conn, cb)
                        }, 1000 * 5 * 60);
                        const d = domain.create();

                        d.run(() => {
                            cb(null, {
                                payload: msg.content.toString(),
                                release: () => {
                                    try {
                                        clearTimeout(timeoutID);
                                        ch.ack(msg);
                                    } catch (e) {
                                        this.__bail(e, conn)
                                    }
                                },
                                reject: () => {
                                    try {
                                        clearTimeout(timeoutID);
                                        ch.nack(msg);
                                    } catch (e) {
                                        this.__bail(e, conn)
                                    }
                                }
                            });
                        })

                        d.on("error", (e) => {
                            setTimeout(() => {
                                this.emit('error', e);
                                //skip error message
                                ch.ack(msg);
                                clearTimeout(timeoutID);
                                //console.error(e.toString())
                            }, 5000)
                        })
                    }
                }, {noAck: false});
            });
        });
    }

    send(msg, queue, cb) {
        return new Promise((resolve, reject) => {
            let payload = JSON.stringify(msg);
            this.__connect(queue, (err, conn) => {
                if (err) {
                    return this.__bail(err, conn, (err)=>{
                        cb && cb(err);
                        reject(err);
                    });
                }

                this.__channel(queue, true, (err, ch) => {
                    if (err) {
                        return this.__bail(err, conn, (err)=>{
                            cb && cb(err);
                            reject(err);
                        })
                    }
                    ch.assertQueue(queue, {durable: true});
                    ch.publish('', queue, new Buffer(payload), {"persistent": true}, (err) => {
                        cb && cb(err);
                        resolve();
                    });
                });
            });
        })
    }

    __connect(name, cb) {
        if (!this.connectString) {
            return setImmediate(() => this.__bail("connect string is empty", null, cb))
        }

        if (this.conn[name]) {
            return setImmediate(() => cb(null, this.conn[name]))
        }
        try {
            amqp.connect(this.connectString, (err, conn) => {
                if (err) {
                    return this.__bail(err, conn, cb)
                }
                this.conn[name] = conn;
                conn.on('close', () => {
                    //console.log("connection closed")
                    this.channelPool[name] = null;
                    this.conn[name] = null;
                    if (this.callback.listen[name]) {
                        setTimeout(() => {
                            this.listen(name, this.callback.listen[name]);
                        }, 5000)
                    }
                });
                conn.on('error', (err) => {
                    this.emit('error', err);
                    //console.log("connection closed by error")
                    this.channelPool[name] = null;
                    this.conn[name] = null;
                    if (this.callback.listen[name]) {
                        setTimeout(() => {
                            this.listen(name, this.callback.listen[name]);
                        }, 5000)
                    }
                });
                cb(null, conn);
            });
        } catch (e) {
            return setImmediate(() => this.__bail(e, null, cb))
        }
    }

    __channel(name, isConfirmChannel, cb) {
        let method = isConfirmChannel ? "createConfirmChannel" : "createChannel";
        if (this.channelPool[name] && this.channelPool[name][method]) {
            return setImmediate(() => cb(null, this.channelPool[name][method]))
        }
        if (!this.conn[name]) {
            return setImmediate(() => cb("channel is closed"));
        }
        try {
            this.conn[name][method]((err, ch) => {
                if (err) {
                    return cb(err)
                }
                this.channelPool[name] = this.channelPool[name] || {};
                this.channelPool[name][method] = this.channelPool[name][method] || {};
                this.channelPool[name][method] = ch;
                cb(null, ch);
            })
        } catch (e) {
            setImmediate(() => cb(e));
        }
    }
}

module.exports = Wrapper;