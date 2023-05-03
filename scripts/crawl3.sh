#!/bin/sh

while true; do
	date
	sleep 30
	lsof -nP -iTCP -sTCP:LISTEN | grep "7001" | wc -l
	if [ "$?" != "0" ]; then
		printf "KVS worker not yet listening!"
		sleep 30
	fi
	date
	java -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar:bin/ cis5550.flame.Master 9000 localhost:7000
done
