package com.mapr.demo.twitter;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

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
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.Client;

public class TweetLogger {

    public static final Logger log = Logger.getLogger(TweetLogger.class);

    /**
     * @param args
     * @throws ServiceException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ServiceException {
        if (args.length < 3) {
            System.out.println("Usage: java -cp <classpath> "
                    + "com.mapr.demo.twitter.TweetLogger "
                    + "<catcherhost> <catcherport> <topic>");
        }

        TweetLogger t = new TweetLogger();
        t.stream(args[2], args[0], Integer.parseInt(args[1]));

    }

    @SuppressWarnings("unused")
    private void query(String query_term, String host, int port)
            throws IOException, ServiceException {

        // Franz client
        Client c = new Client(Lists.newArrayList(new PeerInfo(host, port)));

        // Twitter Client
        Twitter twitter = new TwitterFactory().getInstance();

        // query loop
        try {
            Query query = new Query(query_term);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                System.out.println("count: " + tweets.size());

                for (Status tweet : tweets) {
                    c.sendMessage("users", tweet.getUser().getScreenName());
                    c.sendMessage("tweets", tweet.getText());
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        } catch (Throwable t) {
            System.out.println(t);
            System.exit(-1);
        } finally {
            c.close();
            System.exit(0);
        }

    }

    private class FranzStreamer implements StatusListener {
        private Client c;

        public FranzStreamer(String host, int port) {
            try {
                c = new Client(Lists.newArrayList(new PeerInfo(host, port)));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        public void onStatus(Status s) {
            try {
                c.sendMessage("users", s.getUser().getScreenName());
                c.sendMessage("tweets", s.getText());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        public void onScrubGeo(long userId, long upToStatusId) {
        }

        public void onStallWarning(StallWarning warning) {
        }

        public void onException(Exception ex) {
            ex.printStackTrace();
        }

    }


    public void stream(String query_term, String host, int port) {

        StatusListener listener = new FranzStreamer(host, port);

        TwitterStream ts = new TwitterStreamFactory().getInstance();
        ts.addListener(listener);

        FilterQuery fq = new FilterQuery();
        fq.track(new String[] { query_term });

        ts.filter(fq);

    }

}
