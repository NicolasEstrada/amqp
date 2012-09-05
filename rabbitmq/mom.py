"""Message Oriented Middleware"""

__author__ = "Nicolas, Matias"
__version__ = "0.1"

import time
import logging
import pika
import jsonhandler

import mq
from mq import *


class messageOrientedMiddleware(object):
    def __init__(self, **kw):
        # Init (kw params): host, port, vhost, user, paswd, exchge, durable

        self.queue_name = kw.get('queue', 'test')
        self.routing_keys = kw.get('rkeys', ['test.#'])
        self.callback = kw.get('callback', self.processing)
        self.messages = kw.get('messages', 10)

        self.receiver = mq.MQAsyncReceiver(self.queue_name, self.routing_keys, self.callback, **kw)
        self.publisher = mq.MQSyncSender(**kw)
        self.publisher.connect()
        super(messageOrientedMiddleware, self).__init__()

    def test(self):
        # Test method
        for i in xrange(self.messages):
            msge = jsonhandler.dumps({'message_id': i, 'content': 'store_this'})
            self.publisher.publish('test.rmq.1', msge)
        self.receiver.connect()

    def processing(self, rkey, body):
        print "Received:", rkey, body
        return True

if __name__ == "__main__":

    mtest = messageOrientedMiddleware(messages=10000, durable=True)
    mtest.test()
