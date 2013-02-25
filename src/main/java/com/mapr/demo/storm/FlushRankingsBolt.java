package com.mapr.demo.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.io.Files;
import com.mapr.demo.storm.util.Rankable;
import com.mapr.demo.storm.util.Rankings;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlushRankingsBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3199927072520036231L;
    public static final Logger log = LoggerFactory.getLogger(FlushRankingsBolt.class);
    private OutputCollector collector;
    private String type = "unknown";
    private final Properties props;

    public FlushRankingsBolt(String type) {
        this.type = type;
        props = TweetTopology.loadProperties();
    }

    public void execute(Tuple tuple) {
        Rankings rankings = (Rankings)tuple.getValue(0);

        JSONObject json = new JSONObject();
        List<Rankable> ranks = rankings.getRankings();
        for (Rankable r : ranks) {
            json.put((String) r.getObject(), r.getCount());
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
        try {
            // write to temp copy and rename to get atomic effect
            File outputFile = new File(props.getProperty("doc.root"),
                props.getProperty(type + ".output"));
            File f = new File(props.getProperty("doc.root"),
                props.getProperty(type + ".output")+".tmp");
            Files.write(JSONValue.toJSONString(json), f, Charset.forName("UTF-8"));
            if (!f.renameTo(outputFile)) {
                log.error("Unable to overwrite {} using rename", outputFile);
                throw new IOException("Cannot overwrite data file");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
