#!/bin/sh

while true; do
	date
	sleep 3
	date
	java -cp lib/webserver.jar:lib/kvs.jar:bin/ cis5550.kvs.Worker 7001 worker1 localhost:7000
done
