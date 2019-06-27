"use strict";

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

	listen({ queue, exchange, prefetch = 1, ack = true, durable = true, autoDelete = true }, cb) {
		this.__connect(`${queue}.${exchange}`, (err, conn) => {
			if (err) {
				return cb(err)
			}

			this.__channel(`${queue}.${exchange}`, ack, async (err, ch) => {
				if (err) {
					return cb(err)
				}

				this.callback.listen[`${queue}.${exchange}`] = {
					cb: cb,
					done: true
				};

				if (!queue && !exchange) {
					this.__bail(new Error("queue and exchange are empty"), conn, cb)
				}

				let consumeQueueName
				try {
					if (exchange) {
						await ch.assertExchange(exchange, 'direct', { durable, autoDelete });
						const ok = await ch.assertQueue('', { exclusive: true });
						consumeQueueName = ok.queue
						await ch.bindQueue(ok.queue, exchange, queue);
					} else if (queue) {
						consumeQueueName = queue
						await ch.assertQueue(queue, { durable });
					}
					await ch.prefetch(prefetch);
				} catch (e) {
					this.__bail(e, conn, cb)
				}

				ch.consume(consumeQueueName, (msg) => {
					if (msg) {
						let timeoutID = setTimeout(() => {
							this.__bail(new Error("process frozen"), conn, cb)
						}, 1000 * 5 * 60);
						const d = domain.create();

						d.run(() => {
							cb(null, {
								payload: msg.content.toString(),
								release: () => {
									try {
										clearTimeout(timeoutID);
										ch.ack(msg);
										msg = null;
									} catch (e) {
										this.__bail(e, conn)
									}
								},
								reject: () => {
									try {
										clearTimeout(timeoutID);
										ch.nack(msg);
										msg = null;
									} catch (e) {
										this.__bail(e, conn)
									}
								}
							});
						})

						d.on("error", (e) => {
							setTimeout(() => {
								this.emit('error', e);
								try {
									msg && ch.ack(msg);
								} catch (e) {
									this.__bail(e, conn)
								}
								clearTimeout(timeoutID);
							}, 5000)
						})
					}
				}, { noAck: !ack });
			});
		});
	}

	send({ msg, queue, exchange, persistent = true, ack = true, durable = true, autoDelete = true }, cb) {
		return new Promise((resolve, reject) => {
			let payload = JSON.stringify(msg);
			this.__connect(`${queue}.${exchange}`, (err, conn) => {
				if (err) {
					return this.__bail(err, conn, (err) => {
						cb && cb(err);
						reject(err);
					});
				}

				this.__channel(`${queue}.${exchange}`, ack, async (err, ch) => {
					if (err) {
						return this.__bail(err, conn, (err) => {
							cb && cb(err);
							reject(err);
						})
					}
					if (exchange) {
						await ch.assertExchange(exchange, 'direct', { durable, autoDelete });
						ch.publish(exchange, queue, Buffer.from(payload), { persistent }, (err) => {
							cb && cb(err);
							resolve();
						});
					} else {
						await ch.assertQueue(queue, { durable });
						ch.publish('', queue, Buffer.from(payload), { persistent }, (err) => {
							cb && cb(err);
							resolve();
						});
					}
				});
			});
		})
	}

	__connect(name, cb) {
		if (!this.connectString) {
			return setImmediate(() => this.__bail(new Error("connect string is empty"), null, cb))
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
					this.channelPool[name] = null;
					this.conn[name] = null;
					if (this.callback.listen[name] && this.callback.listen[name].done) {
						this.callback.listen[name].done = false;
						setTimeout(() => {
							if (!this.callback.listen[name].done) {
								this.callback.listen[name].done = true;
								this.listen(name, this.callback.listen[name].cb);
							}
						}, 5000)
					}
				});
				conn.on('error', (err) => {
					this.emit('error', err);
					this.channelPool[name] = null;
					this.conn[name] = null;
					if (this.callback.listen[name] && this.callback.listen[name].done) {
						this.callback.listen[name].done = false;
						setTimeout(() => {
							if (!this.callback.listen[name].done) {
								this.callback.listen[name].done = true;
								this.listen(name, this.callback.listen[name].cb);
							}
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