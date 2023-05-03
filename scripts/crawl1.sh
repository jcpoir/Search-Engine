#!/bin/sh

while true; do
	date
	sleep 1
	java -cp lib/webserver.jar:lib/kvs.jar:bin/ cis5550.kvs.Master 7000
done
