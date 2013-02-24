package com.mapr.demo.storm;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.io.Files;
import com.mapr.demo.storm.util.Rankable;
import com.mapr.demo.storm.util.Rankings;

public class FlushRankingsBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3199927072520036231L;
    public static final Logger Log = Logger.getLogger(FlushRankingsBolt.class);
    private OutputCollector collector;
    private String type = "unknown";

    public FlushRankingsBolt(String type) {
        this.type = type;
    }

    public void execute(Tuple tuple) {
        Rankings rankings = (Rankings)tuple.getValue(0);

        JSONObject json = new JSONObject();
        List<Rankable> ranks = rankings.getRankings();
        Iterator<Rankable> i = ranks.iterator();
        while( i.hasNext() ) {
            Rankable r = i.next();
            json.put((String)r.getObject(), r.getCount());
        }
        flush(json);
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // don't really use this
        declarer.declare(new Fields("data"));
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    private void flush(JSONObject json) {
        // TODO: hardcoded
        File f = new File("/Users/brad/dev/mapr/mapr-spout-test/www/data/" +
                type + ".json");
        try {
            Files.write((CharSequence)JSONValue.toJSONString(json), f,
                    Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
