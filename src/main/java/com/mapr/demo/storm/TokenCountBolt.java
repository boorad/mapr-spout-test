package com.mapr.demo.storm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import net.minidev.json.JSONValue;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.io.Files;

public class TokenCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = -201019832603057184L;
    public static final Logger log = Logger.getLogger(TokenCountBolt.class);
    Map<String, Integer> counts = new HashMap<String, Integer>();
    private OutputCollector _collector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this._collector = collector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);

        // send to another bolt?  meh
        //_collector.emit(new Values(word, count));

        // flush to json file
        flush();

        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    private void flush() {

        File f = new File("/Users/brad/dev/mapr/mapr-spout-test/www/data/words.json");

        try {

            Files.write((CharSequence)JSONValue.toJSONString(counts), f,
                    Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
