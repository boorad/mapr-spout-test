package com.mapr.demo.storm.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class Rankings implements Serializable, Iterable<Rankable> {

    private static final long serialVersionUID = -1549827195410578904L;

    private final int maxSize;

    private final SortedSet<Rankable> data = Collections.synchronizedSortedSet(new TreeSet<Rankable>(Ordering.natural().reverse()));

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

    public Collection<Rankable> getRankings() {
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

    /**
     * Returns an iterator over a set of elements of type T.
     *
     * @return an Iterator.
     */
    public Iterator<Rankable> iterator() {
        return data.iterator();
    }
}
