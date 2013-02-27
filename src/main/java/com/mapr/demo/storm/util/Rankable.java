package com.mapr.demo.storm.util;

import java.util.List;

/**
 * A rankable count.  Must be interface for obscure Stormy reasons.
 */
public interface Rankable<T extends Rankable> extends Comparable<T> {
    Object getObject();

    long getCount();

    List<Object> getFields();

}
