This is a simple webserver app for testing.

python server.py will listen on port 8088.

Apache Benchmark

ab -n 1000 -c 10 http://127.0.0.1:8088/

	-n Number of requests
	-c Concurrency
	web path

Tsung Benchmark

tsung -f tsung_conf.xml start

This command will execute testing based on the tsung_conf.xml file.
