"""Message Oriented Middleware"""

__author__ = "Nicolas, Matias"
__version__ = "0.1"

import os
import sys
import uuid
import time
import logging
from random import choice

import pika
import bjoern
from flask import Flask, jsonify, request

import mq
from mq import *
import jsonhandler

app = Flask(__name__)

N_SHARDS = 10
REQUEST_TYPES = ['AUTH', 'DB_INSERT', 'DB_UPDATE', 'DB_VIEW', 'DB_DELETE']
SHARDS = [i for i in range(N_SHARDS)]


def default(rkey, body):
    print "DEFAULT: ", rkey, body
    time.sleep(0.3)
    return True


def authentication(rkey, body):
    print "AUTH: ", rkey, body
    # INSERT + SELECT
    time.sleep(0.3)
    return True


def db_store(rkey, body):
    print "CRUD: ", rkey, body
    # INSERT
    time.sleep(0.2)
    return True


def db_shards(rkey, body):
    print "SHARDS: ", rkey, body
    # SELECT
    time.sleep(0.1)
    return True


CALLBACKS = {
    "default": default,
    "authentication": authentication,
    "db_store": db_store,
    "db_shards": db_shards
}


class messageOrientedMiddleware(object):
    def __init__(self, **kw):
        # Init (kw params): host, port, vhost, user, paswd, exchge, durable

        self.queue_name = kw.get('queue', 'test')
        self.routing_keys = kw.get('rkeys', ['test.#'])
        self.callback = kw.get('callback', self.processing)

        if 'callback' in kw.keys():
            del kw['callback']
        if 'rkeys' in kw.keys():
            del kw['rkeys']

        self.receiver = mq.MQAsyncReceiver(self.queue_name, self.routing_keys, self.callback, **kw)
        self.publisher = mq.MQSyncSender(**kw)
        self.publisher.connect()
        super(messageOrientedMiddleware, self).__init__()

    def test(self, messages):
        # Test method
        for i in xrange(messages):
            # Routing key defined as: <queue>.<request>.<shard>
            msge = jsonhandler.dumps({'message_id': i, 'content': str(uuid.uuid4().hex)})
            self.publisher.publish('test.{0}.{1}'.format(choice(REQUEST_TYPES), i % N_SHARDS), msge)
        self.receiver.connect()

    def publish(self, rkey, body):
        try:
            self.publisher.publish(rkey, body)
            return True
        except:
            return False

    def consume(self):
        self.receiver.connect()

    def processing(self, rkey, body):
        print "Received:", rkey, body
        return True


@app.route('/')
def index():
    return "<h1> MOM Web Service <h1/>"


@app.route('/mom_ws/<type>', methods=['GET', 'POST'])
def mom_ws(req_type='default'):
    data = request.values.get('data', None)
    if data:
        processing = messageOrientedMiddleware(
                durable=True, queue=req_type,
                callback=CALLBACKS[req_type], rkeys=['test.AUTH.*'])
        if processing.publish(req_type, jsonhandler.loads(str(data))):
            return jsonify({"status": True})
    else:
        return jsonify({"status": False})


@app.route('/mvc_ws/<type>', methods=['GET', 'POST'])
def mvc_ws(req_type='default'):
    data = request.values.get('data', None)
    if data:
        if CALLBACKS[req_type](req_type, jsonhandler.loads(str(data))):
            return jsonify({"status": True})
    else:
        return jsonify({"status": False})


if __name__ == "__main__":

    bjoern.run(app, '127.0.0.1', 8088)

    # if len(sys.argv) != 2:
    #     sys.exit(0)

    # opt = str(sys.argv[1])

    # if opt == str(1):
    #     mtest = messageOrientedMiddleware(
    #         durable=True, queue='auth',
    #         callback=authentication, rkeys=['test.AUTH.*'])
    #     mtest.test(1000)
    # elif opt == str(2):
    #     mtest = messageOrientedMiddleware(
    #         durable=True, queue='db',
    #         callback=db_store, rkeys=['test.DB_INSERT.*'])
    #     mtest.test(1000)
    # elif opt == str(3):
    #     mtest = messageOrientedMiddleware(
    #         durable=True, queue='shards_all',
    #         callback=db_shards, rkeys=['test.*.3'])
    #     mtest.test(1000)
