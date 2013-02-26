#Usage

First compile and install mapr-spout on the demo branch:

    git clone git://github.com/boorad/mapr-spout.git
    cd mapr-spout
    mvn install -DskipTests

Note that the unit tests are known to be broken at present.

Start zookeeper

    You may want to start it on a different port, like 2108.  Change this in <zk>/conf/zoo.cfg and restart

Start the mapr-spout server:

    java -cp mapr-spout/target/mapr-spout-0.1-SNAPSHOT-jar-with-dependencies.jar com.mapr.franz.server.Server <ip-address> <port> [<zk-port>]

Then come back to the mapr-spout-test directory.

    java -cp <classpath> com.mapr.demo.twitter.TweetLogger <catcher_IP> <catcher_port> <topic>

or

    mvn compile
    mvn exec:java -Dexec.mainClass=com.mapr.demo.twitter.TweetLogger -Dexec.args="<catcher_IP> <catcher_port> <topic>"
