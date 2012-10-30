import sys

from pong import application


def main(argv=None):
    if argv:
        if "gevent" in argv:
            from gevent import wsgi
            print "Running Gevent based server on 127.0.0.1:8088"
            wsgi.WSGIServer(('', 8088), application, spawn=None).serve_forever()
        elif "bjoern" in argv:
            import bjoern
            bjoern.listen(application, '127.0.0.1', 8088)
            print "Running Bjoern based server on 127.0.0.1:8088"
            bjoern.run()

    else:
        print "Please provide WS framework to use"


if __name__ == '__main__':
    main(sys.argv)
