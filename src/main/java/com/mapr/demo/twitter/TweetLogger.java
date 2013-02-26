package com.mapr.demo.twitter;

import java.io.IOException;
import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetLogger {

    public static final Logger log = LoggerFactory.getLogger(TweetLogger.class);

    /**
     * Runs the process that queries twitter and logs the results.
     *
     * @param args Arguments include the host where the log catcher is running, the port for same and the twitter query.
     * @throws ServiceException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ServiceException, TwitterException {
        if (args.length < 3) {
            System.out.println("Usage: java -cp <classpath> "
                    + "com.mapr.demo.twitter.TweetLogger "
                    + "<catcherhost> <catcherport> <query-for-twitter>");
        }

        TweetLogger t = new TweetLogger();

//        Client logger = new Client(Lists.newArrayList(new PeerInfo(args[0], Integer.parseInt(args[1]))));
//        log.info("Connected to log catcher");
//        log.info("Running query");
//        t.query(args[2], logger);

        log.info("Invoking filter stream");
        t.stream(args[2], args[0], Integer.parseInt(args[1]));
    }

    @SuppressWarnings("unused")
    private void query(String query_term, Client logger)
            throws IOException, ServiceException, TwitterException {

        // Twitter Client
        Twitter twitter = new TwitterFactory().getInstance();

        // query loop
        try {
            Query query = new Query(query_term);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();

                for (Status tweet : tweets) {
                    String user = tweet.getUser().getScreenName();
                    String content = tweet.getText();
                    log.debug("User {} tweeted {}", user, content);
                    logger.sendMessage("users", user);
                    logger.sendMessage("tweets", content);

                }
            } while ((query = result.nextQuery()) != null);
        } finally {
            logger.close();
        }

    }

    private class FranzStreamer implements StatusListener {
        private Client c;

        public FranzStreamer(String host, int port) throws IOException, ServiceException {
            c = new Client(Lists.newArrayList(new PeerInfo(host, port)));
        }

        public void onStatus(Status s) {
            try {
                c.sendMessage("users", s.getUser().getScreenName());
                c.sendMessage("tweets", s.getText());
            } catch (Exception e) {
                log.error("Exception raised while logging status update", e);
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


    public void stream(String query_term, String host, int port) throws IOException, ServiceException {
        StatusListener listener = new FranzStreamer(host, port);

        TwitterStream ts = new TwitterStreamFactory().getInstance();
        ts.addListener(listener);

        FilterQuery fq = new FilterQuery();
        fq.track(new String[]{query_term});

        ts.filter(fq);
    }

}
