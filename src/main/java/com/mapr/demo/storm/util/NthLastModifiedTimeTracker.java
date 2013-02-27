package com.mapr.demo.storm.util;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import backtype.storm.utils.Time;

/**
 * This class tracks the time-since-last-modify of a "thing" in a rolling fashion.
 *
 * For example, create a 5-slot tracker to track the five most recent time-since-last-modify.
 *
 * You must manually "mark" that the "something" that you want to track -- in terms of modification times -- has just
 * been modified.
 *
 */
public class NthLastModifiedTimeTracker {

    private static final int MILLIS_IN_SEC = 1000;

    private final CircularFifoBuffer lastModifiedTimesMillis;

    public NthLastModifiedTimeTracker(int slots) {
        if (slots < 1) {
            throw new IllegalArgumentException("slots must be greater than zero (you requested " + slots + ")");
        }
        lastModifiedTimesMillis = new CircularFifoBuffer(slots);
        long t = Time.currentTimeMillis();
        for (int i = 0; i < lastModifiedTimesMillis.maxSize(); i++) {
            lastModifiedTimesMillis.add(Long.valueOf(t));
        }
    }

    public int recordModAndReturnOldest() {
        long modifiedTimeMillis = (Long) lastModifiedTimesMillis.get();
        lastModifiedTimesMillis.add(Time.currentTimeMillis());
        return (int) ((Time.currentTimeMillis() - modifiedTimeMillis + MILLIS_IN_SEC / 2) / MILLIS_IN_SEC);
    }

}
