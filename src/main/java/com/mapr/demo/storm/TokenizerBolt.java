package com.mapr.demo.storm;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mapr.demo.twitter.Twokenizer;

public class TokenizerBolt extends BaseRichBolt {

    private static final long serialVersionUID = -7548234692935382708L;
    private Twokenizer twokenizer = new Twokenizer();
    private OutputCollector _collector;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this._collector = collector;
    }

    public void execute(Tuple tuple) {
        String tweet = tuple.getString(0);
        List<String> tokens = twokenizer.twokenize(tweet);
        for (String token: tokens) {
            _collector.emit(tuple, new Values(token));
        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
