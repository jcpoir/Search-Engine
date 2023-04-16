
all:
	javac -d bin -sourcepath src src/cis5550/flame/*.java
	javac -d bin -sourcepath src src/cis5550/generic/*.java
	javac -d bin -sourcepath src src/cis5550/jobs/*.java
	javac -d bin -sourcepath src src/cis5550/kvs/*.java
	javac -d bin -sourcepath src src/cis5550/tools/*.java
	javac -d bin -sourcepath src src/cis5550/webserver/*.java
# javac --source-path src src/cis5550/jobs/Crawler.java
