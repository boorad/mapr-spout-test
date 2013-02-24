package com.mapr.demo.storm;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;

import com.mapr.demo.storm.util.AbstractRankerBolt;
import com.mapr.demo.storm.util.Rankings;

/**
 * This bolt merges incoming {@link Rankings}.
 *
 * It can be used to merge intermediate rankings generated by {@link IntermediateRankingsBolt} into a final,
 * consolidated ranking. To do so, configure this bolt with a globalGrouping on {@link IntermediateRankingsBolt}.
 *
 */
public final class TotalRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = Logger.getLogger(TotalRankingsBolt.class);

    public TotalRankingsBolt() {
        super();
    }

    public TotalRankingsBolt(int topN) {
        super(topN);
    }

    public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    public void updateRankingsWithTuple(Tuple tuple) {
        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        super.getRankings().updateWith(rankingsToBeMerged);
    }

    public Logger getLogger() {
        return LOG;
    }

}