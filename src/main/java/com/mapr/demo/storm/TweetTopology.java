package com.mapr.demo.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.mapr.TailSpout;
import com.mapr.storm.streamparser.CountBlobStreamParserFactory;
import com.mapr.storm.streamparser.StreamParserFactory;

public class TweetTopology {

    private static final String DEFAULT_BASE_DIR = "/tmp/mapr-spout-test";
    private static String baseDir = "";
    public static final Logger Log = Logger.getLogger(TweetTopology.class);
    private static final String PROPERTIES_FILE = "conf/test.properties";

    private static Properties loadProperties() {
        Properties props = new Properties();
        try {
            FileInputStream in = new FileInputStream(PROPERTIES_FILE);
            props.load(in);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static void main(String[] args) throws AlreadyAliveException,
            InvalidTopologyException, InterruptedException {

        Log.info("---------------------");
        Log.info("------STARTING-------");
        Log.info("---------------------");
        Log.info("Building topology");

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        Properties props = TweetTopology.loadProperties();
        boolean remote = Boolean.parseBoolean(props.getProperty("remote"));
        int numSpouts = Integer.parseInt(props.getProperty("spouts"));
        baseDir = props.getProperty("base.directory", DEFAULT_BASE_DIR);

        // init the MapR Tail Spout
        StreamParserFactory spf = new CountBlobStreamParserFactory();
        File statusFile = new File(baseDir + "/status");
        File inDir = new File(baseDir);
        Pattern inPattern = Pattern.compile("tweets");
        TailSpout spout = new TailSpout(spf, statusFile, inDir, inPattern);
        spout.setReliableMode(true);

        topologyBuilder.setSpout("mapr_tail_spout", spout, numSpouts);
        topologyBuilder.setBolt("tokenizer_bolt", new TokenizerBolt(), 1)
            .shuffleGrouping("mapr_tail_spout");
        topologyBuilder.setBolt("counter_bolt", new TokenCountBolt(), 1)
            .fieldsGrouping("tokenizer_bolt", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        Log.info("topology built.");

/*
        // TODO: properties file
        conf.setNumWorkers(300);
        conf.setMaxSpoutPending(5000);
        conf.setMaxTaskParallelism(500);
*/
        if (remote) {
            Log.info("Sleeping 1 seconds before submitting topology");
            Thread.sleep(1000);
            StormSubmitter.submitTopology("mapr-spout-test Topology", conf,
                    topologyBuilder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("mapr-spout-test Local Topology", conf,
                    topologyBuilder.createTopology());

            // TODO: rest of this is for DEV only
            Thread.sleep(600000);

            Log.info("DONE");
            try {
                cluster.shutdown();
                System.exit(0);
            } catch (Exception e) {
                Log.error("Cluster Shutdown Error");
            }

        }

    }
}
