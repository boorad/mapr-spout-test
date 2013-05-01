package com.mapr.demo.storm;


import backtype.storm.tuple.Tuple;

import com.mapr.demo.storm.util.AbstractRankerBolt;
import com.mapr.demo.storm.util.RankableCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt ranks incoming objects by their count.
 *
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 *
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger log = LoggerFactory.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt(int topN) {
        super(topN, true);
    }

    @Override
    public void updateRankingsWithTuple(Tuple tuple) {
        getRankings().add(RankableCount.from(tuple));
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
