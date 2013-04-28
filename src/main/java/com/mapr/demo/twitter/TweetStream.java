package com.mapr.demo.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.demo.twitter.wire.Tweet;
import com.mapr.franz.catcher.Client;

public class TweetStream {

    public static final Logger log = LoggerFactory.getLogger(TweetStream.class);

    private TwitterStream ts;
    private String queryTerm;
    private StatusListener listener;

    public void startStream(StatusListener l)
            throws IOException, ServiceException {
        listener = l;

        ts = new TwitterStreamFactory().getInstance();
        ts.addListener(listener);

        FilterQuery fq = new FilterQuery();
        fq.track(new String[]{queryTerm});

        ts.filter(fq);
    }

    public void stopStream() {
        log.info("Shutting down TwitterStream");
        ts.shutdown();
    }

    public void changeQuery(String query) throws IOException {
        queryTerm = query;
        log.info("query changing to: " + query);

        // stop stream
	stopStream();

        // restart stream with new term

        ts = new TwitterStreamFactory().getInstance();
        ts.addListener(listener);

        FilterQuery fq = new FilterQuery();
        fq.track(new String[]{queryTerm});

        ts.filter(fq);

    }

}
