package com.mapr.demo.storm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mapr.demo.storm.util.NthLastModifiedTimeTracker;
import com.mapr.demo.storm.util.SlidingWindowCounter;
import com.mapr.demo.storm.util.TupleHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt performs moving window counts of incoming objects.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * Each time a tuple arrives, the count for the 0'th element of that tuple is incremented and the current count and
 * age for that key is emitted.  This guarantees any downstream consumer sees updates as soon as they happen.
 * <p/>
 * Each time a tick happens, the entire table is output.  This guarantees that any downstream consumer sees correct
 * counts for symbols not seen since the last tick.  Counts that go to zero are emitted once as zeros and then
 * removed from further consideration until that key reappears.
 */
public class RollingCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger log = LoggerFactory.getLogger(RollingCountBolt.class);

    private SlidingWindowCounter<Object> counter;
    private final int windowLengthInSeconds;
    private final int timeUnit;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCountBolt(int windowLengthInSeconds, int timeUnit) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.timeUnit = timeUnit;
        resetCounter();
    }

    /**
     * Configures tick events to arrive once every slot.
     *
     * @return The updated configuration
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeUnit);
        return conf;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(this.windowLengthInSeconds / this.timeUnit);
    }

    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            //log.debug("Received tick tuple, triggering emit of current window counts");
            int actualWindowLengthInSeconds = lastModifiedTracker.recordModAndReturnOldest();

            SlidingWindowCounter.DatedMap<Object> counts = counter.getCountsAdvanceWindow();
            for (Object key : counts.keySet()) {
                collector.emit(new Values(key, counts.get(key), actualWindowLengthInSeconds, counts.age(key)));
                //log.debug("Periodic dump {} at {}", key, counts.get(key));
            }
        } else if( TupleHelpers.isNewQueryTuple(tuple) ) {
            log.info("new query tuple");
            resetCounter();
            collector.emit(tuple, new Values(TupleHelpers.NEW_QUERY_TOKEN));
            collector.ack(tuple);
        } else {
            Object key = tuple.getValue(0);
            counter.incrementCount(key);
            int actualWindowLengthInSeconds = lastModifiedTracker.recordModAndReturnOldest();
            //log.debug("Bump of {} to {}", key, counter.get(key));
            collector.emit(new Values(key, counter.get(key), actualWindowLengthInSeconds, counter.age(key)));
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds", "age"));
    }

    private void resetCounter() {
        counter = new SlidingWindowCounter<Object>((this.windowLengthInSeconds + this.timeUnit - 1) / this.timeUnit);
    }
}
