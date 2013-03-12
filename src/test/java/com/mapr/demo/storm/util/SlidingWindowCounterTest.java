package com.mapr.demo.storm.util;

import org.junit.Test;

import static junit.framework.Assert.*;

public class SlidingWindowCounterTest {
    @Test
    public void testExpiration() {
        SlidingWindowCounter<String> sw = new SlidingWindowCounter<String>(4);

        // counts go up for a bit
        sw.incrementCount("x");
        sw.incrementCount("x");
        sw.incrementCount("y");

        SlidingWindowCounter.DatedMap<String> tbl = sw.getCountsAdvanceWindow();
        assertEquals(2, tbl.get("x"));
        assertEquals(1, tbl.get("y"));
        assertEquals(0, tbl.age("x"));
        assertEquals(0, tbl.age("y"));
        sw.incrementCount("x");

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(3, tbl.get("x"));
        assertEquals(1, tbl.get("y"));
        assertEquals(0, tbl.age("x"));
        assertEquals(1, tbl.age("y"));
        sw.incrementCount("x");

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(4, tbl.get("x"));
        assertEquals(1, tbl.get("y"));
        assertEquals(0, tbl.age("x"));
        assertEquals(2, tbl.age("y"));
        sw.incrementCount("x");
        sw.incrementCount("x");

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(6, tbl.get("x"));
        assertEquals(1, tbl.get("y"));
        assertEquals(0, tbl.age("x"));
        assertEquals(3, tbl.age("y"));

        // then as windows no long have that symbol, the counts go down
        tbl = sw.getCountsAdvanceWindow();
        assertEquals(4, tbl.get("x"));
        assertEquals(0, tbl.get("y"));
        assertEquals(1, tbl.age("x"));
        assertEquals(5, tbl.age("y"));

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(3, tbl.get("x"));
        assertEquals(0, tbl.get("y"));
        assertEquals(2, tbl.age("x"));
        assertEquals(5, tbl.age("y"));

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(2, tbl.get("x"));
        assertEquals(3, tbl.age("x"));

        // verify we see a zero count at least once
        tbl = sw.getCountsAdvanceWindow();
        assertEquals(0,tbl.get("x"));
        assertTrue(tbl.keySet().contains("x"));

        tbl = sw.getCountsAdvanceWindow();
        assertEquals(0, tbl.get("x"));
        assertFalse(tbl.keySet().contains("x"));
    }
}
