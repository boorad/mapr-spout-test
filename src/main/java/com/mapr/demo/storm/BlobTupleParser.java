package com.mapr.demo.storm;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.mapr.ProtoSpout.TupleParser;

public class BlobTupleParser extends TupleParser implements Serializable {

    private static final long serialVersionUID = 3297543209369011198L;

    @Override
    public List<Object> parse(ByteString buffer) {
        return Lists.<Object> newArrayList(buffer.toByteArray());
    }

    @Override
    public Fields getOutputFields() {
//        throw  new UnsupportedOperationException("Default operation");
        return new Fields("msg");
    }

}
