package com.mapr.demo.storm;

import java.util.List;

import backtype.storm.tuple.Fields;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.mapr.ProtoSpout.TupleParser;

public class BlobTupleParser extends TupleParser {

    Splitter onSpace = Splitter.on(" ");

    @Override
    public List<Object> parse(ByteString buffer) {
        return Lists.<Object> newArrayList(onSpace.split(buffer.toStringUtf8()));
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("content");
    }

}
