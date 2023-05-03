#!/bin/sh

if [ "$1" = "" ]; then
	printf "Must specify a port number for Flame worker\n"
	exit 1
fi

while true; do
	date
	sleep 30
	lsof -nP -iTCP -sTCP:LISTEN | grep "8001" | wc -l
	if [ "$?" != "0" ]; then
		printf "KVS worker not yet listening!"
		sleep 33
	fi
	java -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar:bin/ cis5550.flame.Worker $1 localhost:9000
done
