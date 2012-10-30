from gevent import wsgi
from pong import application

print "Running on 127.0.0.1:8088"
wsgi.WSGIServer(('', 8088), application, spawn=None).serve_forever()
