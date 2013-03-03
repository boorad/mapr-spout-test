package com.mapr.demo.storm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRawDataBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Logger log= LoggerFactory.getLogger(WriteRawDataBolt.class);
    private AtomicInteger tupleCount = new AtomicInteger();
    private Properties props;
    private File dataDirectory;
    private int fileNameInterval;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        props = TweetTopology.loadProperties();
        dataDirectory = new File(props.getProperty("tweet.data", "/tmp"));
        dataDirectory.mkdirs();
        fileNameInterval = Integer.parseInt(props.getProperty("tweet.data.interval", "100000"));
    }

    public void execute(Tuple tuple) {
        int n = tupleCount.incrementAndGet();
        if (n % 1000 == 0) {
            log.warn("Wrote {} tweets", n);
        }
        String tweet = tuple.getString(0);
        try {
            File tmp = new File(dataDirectory, "tweet-" + (System.currentTimeMillis() / fileNameInterval) + ".txt");
            Files.append(tweet + "\n", tmp, Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }

}
