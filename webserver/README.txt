This is a simple webserver app for testing.

Gevent web server
python server.py gevent

Bjoern web server
python server.py bjoern

Gunicorn web server
gunicorn -b :8088 -w 1 pong:application
	-b bind port
	-w number of workers


Apache Benchmark

ab -n 1000 -c 10 http://127.0.0.1:8088/

	-n Number of requests
	-c Concurrency
	web path

Tsung Benchmark

tsung -f tsung_conf.xml start

This command will execute testing based on the tsung_conf.xml file.
