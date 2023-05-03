#!/bin/sh

cd ~/cis553/23sp-CIS5550-InterLinked1/HW8

if [ "$1" = "" ]; then
	printf "Must provide a seed!\n"
	exit
fi

while true; do
	printf "New crawl iteration\n"
	date
	rm job-*.jar
	rm __worker*.jar
	rm worker1/job-*.table
	sleep 1
	make crawl CRAWL_URL=$1 &

	# Run the crawler for half an hour
	date
	sleep 1800

	# Kill everything Java related, since things seem to get stuck after a while for some reason, not sure why.
	# Even if they didn't, this needs to be done periodically to free memory or we'll run out of it.
	printf "Killing everything...\n"
	killall -s SIGINT java

	# Give a few seconds for everything to start up again
	sleep 65
	lsof -nP -iTCP -sTCP:LISTEN | grep "8001" | wc -l
	if [ "$?" != "0" ]; then
		printf "KVS worker not yet listening!"
	fi
	# Now, start everything over again and keep going where we left off (hopefully)
done
