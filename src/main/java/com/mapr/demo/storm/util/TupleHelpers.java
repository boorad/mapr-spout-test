package com.mapr.demo.storm.util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public final class TupleHelpers {

    public static final String NEW_QUERY_TOKEN = "{newquery}";

    private TupleHelpers() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static boolean isNewQueryTuple(Tuple tuple) {
        return tuple.getString(0).equals(NEW_QUERY_TOKEN);
    }

}
