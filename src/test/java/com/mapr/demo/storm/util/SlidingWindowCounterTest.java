package com.mapr.demo.storm.util;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class SlidingWindowCounterTest {
    @Test
    public void testExpiration() {
        SlidingWindowCounter<String> sw = new SlidingWindowCounter<String>(4);

        // counts go up for a bit
        sw.incrementCount("x");
        sw.incrementCount("x");
        assertEquals(new Long(2), sw.getCountsThenAdvanceWindow().get("x"));
        sw.incrementCount("x");
        assertEquals(new Long(3), sw.getCountsThenAdvanceWindow().get("x"));
        sw.incrementCount("x");
        assertEquals(new Long(4), sw.getCountsThenAdvanceWindow().get("x"));
        sw.incrementCount("x");
        sw.incrementCount("x");
        assertEquals(new Long(6), sw.getCountsThenAdvanceWindow().get("x"));

        // then as windows no long have that symbol, the counts go down
        assertEquals(new Long(4), sw.getCountsThenAdvanceWindow().get("x"));
        assertEquals(new Long(3), sw.getCountsThenAdvanceWindow().get("x"));
        assertEquals(new Long(2), sw.getCountsThenAdvanceWindow().get("x"));

        // verify we see a zero count for a while
        assertEquals(new Long(0), sw.getCountsThenAdvanceWindow().get("x"));
        assertEquals(new Long(0), sw.getCountsThenAdvanceWindow().get("x"));
        assertEquals(new Long(0), sw.getCountsThenAdvanceWindow().get("x"));
        assertEquals(new Long(0), sw.getCountsThenAdvanceWindow().get("x"));
        assertNull(sw.getCountsThenAdvanceWindow().get("x"));
    }
}
