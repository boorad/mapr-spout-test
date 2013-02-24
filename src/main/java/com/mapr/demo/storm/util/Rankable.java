package com.mapr.demo.storm.util;

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

}
