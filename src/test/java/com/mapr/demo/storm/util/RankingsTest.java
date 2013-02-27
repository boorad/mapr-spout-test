package com.mapr.demo.storm.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RankingsTest {
    @Test
    public void testSize() {
        Rankings x = new Rankings(5);
        x.add(new RankableCount("x", 4));
        assertEquals(1, x.size());

        x.add(new RankableCount("y", 1));
        assertEquals(2, x.size());

        x.add(new RankableCount("x", 6));
        assertEquals(2, x.size());

        x.add(new RankableCount("z", 5));
        assertEquals(3, x.size());

        x.add(new RankableCount("a", 3));
        assertEquals(4, x.size());

        x.add(new RankableCount("b", 2));
        assertEquals(5, x.size());

        x.add(new RankableCount("c", 7));
        assertEquals(5, x.size());

        assertEquals(5, x.getRankings().size());
        List<String> r = Lists.newArrayList();
        for (Rankable z : x.getRankings()) {
            r.add((String) z.getObject());
        }
        assertEquals("[c, x, z, a, b]", r.toString());

        x.add(new RankableCount("c", 0));
        x.add(new RankableCount("a", 0));
        r.clear();
        for (Rankable z : x.getRankings()) {
            r.add((String) z.getObject());
        }
        assertEquals("[x, z, b, y]", r.toString());
        assertEquals(4, x.size());
    }
}
