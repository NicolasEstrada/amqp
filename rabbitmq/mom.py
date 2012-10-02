"""Message Oriented Middleware"""

__author__ = "Nicolas, Matias"
__version__ = "0.1"

import os
import sys
import uuid
import time
import logging
import pika
import jsonhandler
from random import choice

import mq
from mq import *

N_SHARDS = 10
REQUEST_TYPES = ['AUTH', 'DB_INSERT', 'DB_UPDATE', 'DB_VIEW']
SHARDS = [i for i in range(N_SHARDS)]


class messageOrientedMiddleware(object):
    def __init__(self, **kw):
        # Init (kw params): host, port, vhost, user, paswd, exchge, durable

        self.queue_name = kw.get('queue', 'test')
        self.routing_keys = kw.get('rkeys', ['test.#'])
        self.callback = kw.get('callback', self.processing)
        self.messages = kw.get('messages', 10)

        if kw.has_key('callback'):
            del kw['callback']
        if kw.has_key('rkeys'):
            del kw['rkeys']

        self.receiver = mq.MQAsyncReceiver(self.queue_name, self.routing_keys, self.callback, **kw)
        self.publisher = mq.MQSyncSender(**kw)
        self.publisher.connect()
        super(messageOrientedMiddleware, self).__init__()

    def test(self):
        # Test method
        for i in xrange(self.messages):
            # Routing key defined as: <queue>.<request>.<shard>
            msge = jsonhandler.dumps({'message_id': i, 'content': str(uuid.uuid4().hex)})
            self.publisher.publish('test.{0}.{1}'.format(choice(REQUEST_TYPES), i % N_SHARDS), msge)
        self.receiver.connect()

    def processing(self, rkey, body):
        print "Received:", rkey, body
        return True


def authentication(rkey, body):
    print "AUTH: ", rkey, body
    return True


def db_store(rkey, body):
    print "CRUD: ", rkey, body
    return True


def db_shards(rkey, body):
    print "SHARDS: ", rkey, body
    return True

if __name__ == "__main__":

    if len(sys.argv) != 2:
        sys.exit(0)

    opt = str(sys.argv[1])

    if opt == str(1):
        mtest = messageOrientedMiddleware(
            messages=1000, durable=True, queue='auth',
            callback=authentication, rkeys=['test.AUTH.*'])
        mtest.test()
    elif opt == str(2):
        mtest = messageOrientedMiddleware(
            messages=1000, durable=True, queue='db',
            callback=db_store, rkeys=['test.DB_INSERT.*'])
        mtest.test()
    elif opt == str(3):
        mtest = messageOrientedMiddleware(
            messages=1000, durable=True, queue='shards',
            callback=db_shards, rkeys=['test.AUTH.3'])
        mtest.test()
