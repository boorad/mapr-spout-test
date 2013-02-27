package com.mapr.demo.storm;

import java.io.*;
import java.util.Properties;
import java.util.regex.Pattern;

import com.google.common.io.Resources;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetTopology {

    private static final String FILETYPE = "tweets";
    private static final String DEFAULT_TOP_N = "150";
    private static final String DEFAULT_BASE_DIR = "/tmp/mapr-spout-test";
    private static String baseDir = "";
    public static final Logger log = LoggerFactory.getLogger(TweetTopology.class);
    private static final String PROPERTIES_FILE = "conf/test.properties";

    public static Properties loadProperties() {
        Properties props = new Properties();
        try {
            InputStream base = Resources.getResource("base.properties").openStream();
            props.load(base);
            base.close();

            File propFile = new File(PROPERTIES_FILE);
            if (propFile.exists()) {
                log.debug("Adding additional properties from {}", propFile.getCanonicalPath());

                FileInputStream in = new FileInputStream(PROPERTIES_FILE);
                props.load(in);
                in.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static void main(String[] args) throws AlreadyAliveException,
            InvalidTopologyException, InterruptedException {

        log.info("---------------------");
        log.info("------STARTING-------");
        log.info("---------------------");
        log.info("Building topology");

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        Properties props = TweetTopology.loadProperties();
        boolean remote = Boolean.parseBoolean(props.getProperty("remote"));
        int numSpouts = Integer.parseInt(props.getProperty("spouts"));
        baseDir = props.getProperty("base.directory", DEFAULT_BASE_DIR);
        int top_n = Integer.parseInt(props.getProperty("top.n", DEFAULT_TOP_N));

        // init the MapR Tail Spout
        StreamParserFactory spf = new CountBlobStreamParserFactory();
        File statusFile = new File(baseDir + "/status_" + FILETYPE);
        File inDir = new File(baseDir);
        Pattern inPattern = Pattern.compile(FILETYPE);
        TailSpout spout = new TailSpout(spf, statusFile, inDir, inPattern);

        // TODO this should be set to true, but somebody isn't acking tuples correctly and that causes hangs
        spout.setReliableMode(false);

        topologyBuilder.setSpout("mapr_tail_spout", spout, numSpouts);
        topologyBuilder.setBolt("tokenizer", new TokenizerBolt(), 1)
                .shuffleGrouping("mapr_tail_spout");
        topologyBuilder.setBolt("rolling_count", new RollingCountBolt(Integer.parseInt(props.getProperty("window", "3600")), 5), 1)
                .fieldsGrouping("tokenizer", new Fields("word"));
        topologyBuilder.setBolt("intermediate_rank", new IntermediateRankingsBolt(top_n), 1)
                .globalGrouping("rolling_count");
        topologyBuilder.setBolt("flush", new FlushRankingsBolt(FILETYPE), 1)
                .globalGrouping("intermediate_rank");

        Config conf = new Config();
        conf.setDebug(true);

        log.info("topology built.");

/*
        // TODO: properties file
        conf.setNumWorkers(300);
        conf.setMaxSpoutPending(5000);
        conf.setMaxTaskParallelism(500);
*/
        if (remote) {
            log.info("Sleeping 1 seconds before submitting topology");
            Thread.sleep(1000);
            StormSubmitter.submitTopology("mapr-spout-test Tweet Topology",
                    conf, topologyBuilder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("mapr-spout-test Local Tweet Topology",
                    conf, topologyBuilder.createTopology());
/*
            // TODO: rest of this is for DEV only
            Thread.sleep(600000);

            log.info("DONE");
            cluster.shutdown();
            System.exit(0);
*/
        }
    }
}
