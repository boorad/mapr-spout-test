package com.mapr.demo.storm.util;

import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class Rankings implements Serializable {

    private static final long serialVersionUID = -1549827195410578904L;

    private final int maxSize;

    private final SortedSet<Rankable> data = Sets.newTreeSet(Ordering.natural().reverse());

    public Rankings(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1");
        }
        maxSize = topN;
    }

    /**
     * @return the number (size) of ranked objects this instance is currently holding
     */
    public int size() {
        return data.size();
    }

    public List<Rankable> getRankings() {
        return Lists.newArrayList(data);
    }

    public void addAll(Rankings other) {
        for (Rankable r : other.data) {
            add(r);
        }
    }

    public void add(Rankable r) {
        if (r.getCount() > 0) {
            data.add(r);
            while (data.size() > maxSize) {
                data.remove(data.last());
            }
        } else {
            data.remove(r);
        }
    }

    public String toString() {
        return data.toString();
    }
}
