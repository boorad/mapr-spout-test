package com.mapr.demo.storm.util;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import com.google.common.collect.Lists;
import com.mapr.demo.storm.TokenizerBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This abstract bolt provides the basic behavior of bolts that rank objects according to their count.
 * <p/>
 * It uses a template method design pattern for {@link AbstractRankerBolt#execute(Tuple, BasicOutputCollector)} to allow
 * actual bolt implementations to specify how incoming tuples are processed, i.e. how the objects embedded within those
 * tuples are retrieved and counted.
 */
public abstract class AbstractRankerBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 4931640198501530202L;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 5;

    private Logger log = LoggerFactory.getLogger(AbstractRankerBolt.class);
    private final int emitFrequencyInSeconds;
    private Rankings rankings;
    protected boolean reportRankings = false;
    private long lastEmission = 0;
    private int topN = 150;

    public AbstractRankerBolt(int topN, boolean reportRankings) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS, reportRankings);
    }

    public AbstractRankerBolt(int topN, int emitFrequencyInSeconds, boolean reportRankings) {
        this.reportRankings = reportRankings;
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException("The emit frequency must be >= 1 seconds (you requested "
                    + emitFrequencyInSeconds + " seconds)");
        }
        this.topN = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(topN);
    }

    protected Rankings getRankings() {
        return rankings;
    }

    /**
     * This method functions as a template method (design pattern).
     */
    public final void execute(Tuple tuple, BasicOutputCollector collector) {
        if (TupleHelpers.isTickTuple(tuple)) {
            //getLogger().debug("Received tick tuple, triggering emit of current rankings");
            emitRankings(collector);
        } else if( TupleHelpers.isNewQueryTuple(tuple) ) {
            log.info("new query tuple");
            clearRankings();
        } else {
            updateRankingsWithTuple(tuple);
            if (Time.currentTimeMillis() - lastEmission > 2000) {
                emitRankings(collector);
            }
        }
    }

    public abstract void updateRankingsWithTuple(Tuple tuple);

    @SuppressWarnings("rawtypes")
    private void emitRankings(BasicOutputCollector collector) {
        lastEmission = Time.currentTimeMillis();
        collector.emit(new Values(rankings));
        if (reportRankings) {
            List<Rankable> r = Lists.newArrayList();
            for (Rankable rx : rankings) {
                r.add(rx);
                if (r.size() > 20) {
                    break;
                }
            }
            //getLogger().warn("Rankings: " + rankings);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rankings"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    protected void clearRankings() {
        rankings = new Rankings(topN);
    }

    public abstract Logger getLogger();
}
