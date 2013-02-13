package com.mapr.demo.storm;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3199927072520036231L;
	public static final Logger Log = Logger.getLogger(PrintBolt.class);
    private OutputCollector collector;
    private Properties props;
    
    public PrintBolt(Properties props) {
        this.setProps(props);
    }

    public void execute(Tuple tuple) {
        String input = tuple.getString(0);
        Log.debug("input: " + input);
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
        Log.info("Initialized bolt");
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }
}
