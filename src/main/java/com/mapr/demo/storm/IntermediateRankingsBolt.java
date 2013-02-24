package com.mapr.demo.storm;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;

import com.mapr.demo.storm.util.AbstractRankerBolt;
import com.mapr.demo.storm.util.Rankable;
import com.mapr.demo.storm.util.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 *
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 *
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt() {
        super();
    }

    public IntermediateRankingsBolt(int topN) {
        super(topN);
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    public void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        super.getRankings().updateWith(rankable);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
