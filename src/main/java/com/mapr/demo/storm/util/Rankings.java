package com.mapr.demo.storm.util;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.*;

public class Rankings implements Serializable, Iterable<Rankable> {

    private static final long serialVersionUID = -1549827195410578904L;

    private final int maxSize;

    private final Map<Object, Rankable> data = Collections.synchronizedMap(new HashMap<Object, Rankable>());

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
        return Math.min(maxSize, data.size());
    }

    public Collection<Rankable> getRankings() {
        SortedSet<Rankable> r = Sets.newTreeSet(Ordering.natural().reverse());
        r.addAll(data.values());
        while (r.size() > maxSize) {
            r.remove(r.last());
        }
        return r;
    }

    public void add(Rankable r) {
        if (r.getCount() > 0) {
            data.put(r.getObject(), r);
        } else {
            data.remove(r.getObject());
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
        return getRankings().iterator();
    }
}
