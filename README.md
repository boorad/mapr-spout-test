



#Usage

#Preliminaries

Install maven, git and java:

    sudo apt-get update
    sudo apt-get -y install maven
    sudo apt-get -y isntall openjdk-7-jdk
    sudo apt-get -y install git
    sudo apt-get -y install protobuf-compiler

Use 

    sudo update-alternatives --config java

to select java 7.  If you want to set it without interaction, try:

    sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

#Start zookeeper

The easiest way to get ZK running on many systems like ubuntu is to simply install the zookeeperd 
package and start the service.

    sudo apt-get -y install zookeeperd

After you do this, zookeeper should be running on the standard port of 2181.  Zookeeper is 
not needed for the SimpleCatcher used in the demo so you probably can just skip this step.

#Download source and compile

First compile and install mapr-spout on the demo branch:

    git clone git://github.com/boorad/mapr-spout.git
    cd mapr-spout
    mvn install -DskipTests

You can run the tests if you like.  They should take about a minute to run and should complete successfully.  
Note that there are some scaring looking log outputs along the way as various failure modes are tested.

Note also that the first time you compile mapr-spout all kinds of dependencies will be downloaded.  This should
go much faster the second time around.

#Start the catcher server:

    java -cp target/mapr-storm-0.4-jar-with-dependencies.jar com.mapr.franz.simple.SimpleCatcher -port 5100

Then come back to the mapr-spout-test directory.

    java -cp <classpath> com.mapr.demo.twitter.TweetLogger <catcher_IP> <catcher_port> <topic>

or

    mvn compile
    mvn exec:java -Dexec.mainClass=com.mapr.demo.twitter.TweetLogger -Dexec.args="<catcher_IP> <catcher_port> <topic>"
