package com.mapr.demo.storm;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import net.minidev.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.mapr.demo.storm.util.JSONWriter;
import com.mapr.demo.storm.util.Rankable;
import com.mapr.demo.storm.util.Rankings;

public class FlushRankingsBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3199927072520036231L;
    public static final Logger log = LoggerFactory.getLogger(FlushRankingsBolt.class);
    private OutputCollector collector;
    private String type = "unknown.output";

    public FlushRankingsBolt(String type) {
        this.type = type;
    }

    @SuppressWarnings("rawtypes")
    public void execute(Tuple tuple) {
        Rankings rankings = (Rankings) tuple.getValue(0);

        JSONObject json = new JSONObject();
        Collection<Rankable> ranks = rankings.getRankings();
        for (Rankable r : ranks) {
            json.put((String) r.getObject(), r.getCount());
        }
        JSONWriter.write(json, type);
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


}
