
all: generic jobs kvs tools webserver flame

generic: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/generic/*.java

jobs: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/jobs/*.java

kvs: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/kvs/*.java

tools: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/tools/*.java

webserver: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/webserver/*.java

flame: 
	javac -Xdiags:verbose -d bin -sourcepath src src/cis5550/flame/*.java

clean:
	rm -f src/cis5550/generic/*.class
	rm -f src/cis5550/jobs/*.class
	rm -f src/cis5550/kvs/*.class
	rm -f src/cis5550/tools/*.class
	rm -f src/cis5550/webserver/*.class
	rm -f src/cis5550/flame/*.class
	rm -f bin/cis5550/generic/*.class
	rm -f bin/cis5550/jobs/*.class
	rm -f bin/cis5550/kvs/*.class
	rm -f bin/cis5550/tools/*.class
	rm -f bin/cis5550/webserver/*.class
	rm -f bin/cis5550/flame/*.class

crawl:
	jar -cvf crawler.jar bin/cis5550/jobs/Crawler.class bin/cis5550/tools/Hasher.class
	java -cp bin cis5550.flame.FlameSubmit localhost:9000 cis5550.jobs.Crawler http://simple.crawltest.cis5550.net/

# javac --source-path src src/cis5550/jobs/Crawler.java
