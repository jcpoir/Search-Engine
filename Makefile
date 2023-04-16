
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

# javac --source-path src src/cis5550/jobs/Crawler.java
