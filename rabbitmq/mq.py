"""MQ - Module that makes it easy to handle messaging over long periods of time. Ideal for deamons.

This module handles messaging through RabbitMQ. You can both send and receive messages with this module.
"""

__author__ = "Nico, Sebastiaan"
__version__ = "0.1"

import time
import logging
import pika
import jsonhandler
from pika.adapters import SelectConnection
from pika.reconnection_strategies import SimpleReconnectionStrategy

RMQ_SLEEP = 1

class MQ(object):
    """This is a class that combines the functions required for both
    receiving and sending messages through RabbitMQ.
    """

    def __init__(self, **kw):
        """The following parameters can be set:
        host: ip of RabbitMQ host (127.0.0.1)
        port: port of RabbitMQ host (3306)
        vhost: RabbitMQ vhost (/)
        user: RabbitMq user (guest)
        password: RabbitMQ password (guest)
        exchange_name: RabbitMQ exchange ('test_exchange')
        """

        # Settings
        self.host = kw.get('host', '127.0.0.1')
        self.port = kw.get('port', 5672)
        self.vhost = kw.get('vhost', '/')
        self.user = kw.get('user', 'guest')
        self.password = kw.get('password', 'guest')
        self.exchange_name = kw.get('exchange_name', 'test_exchange')

        # Internal parameters
        self.exchange_type = 'topic'
        self._parameters = None
        self._reconnection = None
        self._connection = None
        self._connecting = False
        self._connected = False
        self._closing = False
        self._channel = None

        super(MQ, self).__init__()

    def connect(self):
        """Call this function after the creation of the MQ object in order to 
        start the connection"""

        if self._connecting or self._connected:
            logging.debug("Already connecting to RabbitMQ ...")
            return

        self._connecting = True
        self._parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=pika.PlainCredentials(self.user, self.password))
        self._reconnection = SimpleReconnectionStrategy()

        # Connect to RabbitMQ
        try:
            logging.debug("Connecting to RabbitMQ ...")
            self._connection = SelectConnection(
                                    self._parameters,
                                    self._on_connected,
                                    reconnection_strategy=self._reconnection)
            self._connection.ioloop.start()
        except Exception as e:
            self._connecting = False
            logging.warning('Failed connecting to RabbitMQ. Reason: {0}'.format(e))

    def _on_connected(self, connection):
        """Called when we are fully connected to the RabbitMQ server"""       
        logging.info("Connected to RabbitMQ using host: {0}, port: {1}".format(
            self.host,
            self.port))
        self._connection = connection
        self._connection.add_on_close_callback(self._on_close_connection)
        self._connection.channel(self._on_channel_open)

    def _on_close_connection(self, connection):
        """Called when the connection to RabbitMQ is closed"""
        self._connected = False
        self._connecting = False

        if self._closing:
            logging.info("Connection to RabbitMQ is being closed right now ...")
            if self._connection:
                self._connection.ioloop.stop()
                self._connection = None
        else:
            logging.warning("Connection lost, reconnecting to RabbitMQ ...")
            while not self._connected:
                if not self._connecting:
                    try:
                        self.connect()
                    except:
                        logging.warning("Can't restablish connection with RabbitMQ. Retrying in %s", RMQ_SLEEP)
                        self._connecting = False
                time.sleep(RMQ_SLEEP)

    def _on_channel_open(self, new_channel):
        """This function is called when the channel is opened"""
        logging.debug("Channel open, declaring exchange [{0}]".format(self.exchange_name))
        self._channel = new_channel
        
        # Declaring topic exchange
        self._channel.exchange_declare(
            exchange=self.exchange_name,
            type=self.exchange_type,
            callback=self._on_exchange_declared)
    
    def _on_exchange_declared(self, exchange):
        self._connecting = False
        self._connected = True
        logging.debug("Exchange [{0}] declared".format(self.exchange_name))
        self._exchange = exchange
        self._setup()

    def _setup():
        raise NotImplementedError, "This function should be defined in the parent class"

class MQAsyncReceiver(MQ):
    """This class implements an asynchronous message receiver.
    """
    
    def __init__(self, queue_name, rkeys, callback, **kw):
        """ Initialize the Asynchronous receiver.
        Input is the queue name, an array of 'topic' keys, and a callback function.
        The callback function should have the form callback(routing_key, python_object)."""
        self.queue_name = queue_name
        self.queue_rkeys = rkeys
        self.callback = callback
        self.queue_durable = kw.get('durable', False)
        super(MQAsyncReceiver, self).__init__(**kw)

    def __del__(self):
        if self._connection is not None and self._connection.is_open:
            logging.debug("Closing connection to RabbitMQ")
            self._connection.close()
            self._connection.ioloop.stop()

    def _setup(self):
        logging.debug("Setting up receiver")
        if self.queue_durable:
            self._channel.queue_declare(queue=self.queue_name, durable=True,
                exclusive=False, auto_delete=False,
                callback=self._on_queue_declared)
        else:
            self._channel.queue_declare(queue=self.queue_name, durable=False,
                exclusive=True, auto_delete=True,
                callback=self._on_queue_declared)

    def _on_queue_declared(self, frame):
        logging.debug("Queue declared...")
        for rkey in self.queue_rkeys:
            self._channel.queue_bind(queue=self.queue_name, 
                   exchange=self.exchange_name, 
                   routing_key=rkey)
        self._channel.basic_qos(prefetch_count=20)
        self._channel.basic_consume(self._on_message, queue=self.queue_name)
        logging.info("Receiver checking in and ready to go.")

    def _on_message(self, channel, method_frame, header_frame, body):
        try:
            if self.callback(method_frame.routing_key, jsonhandler.loads(body)):
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except:
            print "Error!"

class MQSyncSender(MQ):
    def __init__(self, **kw):
        """See MQ object"""
        super(MQSyncSender, self).__init__(**kw)

    def __del__(self):
        """This function cleans up the connection to RabbitMQ."""
        if self._connection is not None and self._connection.is_open:
            logging.debug("Closing connection to RabbitMQ")
            self._connection.close()

    def _setup(self):
        #Stop the ioloop such that we can do stuff in the code that initiated the object"""
        self._connection.ioloop.stop()

    def publish(self, routing_key, body):
        """Call this function to publish messages to the RabbitMQ exchange.
        routing_key: a string that describes the message content, dot separated (for example event.555)
        body: the content of the message. Will be converted to JSON"""
        if not self._connection:
            logging.warning("No open RabbitMQ connection!")
            if not self._connecting:
                self.connect()
        # Handle the case where we are not connected to RMQ
        while not self._connected:
            logging.warning("Waiting RabbitMQ reconnecting ...")
            time.sleep(RMQ_SLEEP)
        
        self._channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=jsonhandler.dumps(body),
            properties=pika.BasicProperties(
                content_type="text/plain",
                delivery_mode=2)
            )
        logging.debug("Message sent to routing_key = {0}, exchange = {1}".format(
                                                                            routing_key,
                                                                            self.exchange_name))

def test_receiver(rkey, body):
    print "Received:", rkey, body
    return True

if __name__ == "__main__":
    import random, string
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Testing the MQ package. Please make sure there is a rabbitMQ server on localhost.")

    rkey = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(5)) + "." + ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(5))
    rkey = 'test.'+rkey
    content = "This shit is bananas [" + ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(5)) + "]"
    logging.info("Sending one message with randomly generated routing key [%s] and random content [%s]", rkey, content)
    
    logging.info("Set up the sender connection object")
    sendmq = MQSyncSender()
    sendmq.connect()
    assert sendmq._connection, "There is no connection!"

    logging.info("Test #1: Sending and receiving a normal message")
    sendmq.publish(rkey, content)


    time.sleep(2)
    logging.info("Test #2: being disconnected when sending messages")
    for i in range(0,5):
        sendmq.publish(rkey, content)
        time.sleep(1)
    logging.info("Now kill the connection! Quick!!")
    time.sleep(1)
    logging.info("Bring the connection back in a few seconds please ...")
    for i in range(0,5):
        sendmq.publish(rkey, content)
        time.sleep(1)
    logging.info("Done.")

    logging.info("Testing being disconnected while receiving. Setting up receiver...")
    recmq = MQAsyncReceiver("testreceivingqueue", ['test.#'], test_receiver, durable=True)
    recmq.connect()



