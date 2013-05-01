package com.mapr.demo.twitter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import com.google.protobuf.ServiceException;

public class TweetStream {

    public static final Logger log = LoggerFactory.getLogger(TweetStream.class);

    private TwitterStream ts;
    private String queryTerm;
    private StatusListener listener;

    public void startStream(StatusListener l)
            throws IOException, ServiceException {
        log.info("Starting up TwitterStream");
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

    public void changeQuery(String query) throws IOException, ServiceException {
        queryTerm = query;
        log.info("query changing to: " + query);

        // stop stream
        stopStream();

        // restart stream with new term
        startStream(listener);
    }

}
