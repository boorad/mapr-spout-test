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

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.demo.twitter.wire.Tweet;
import com.mapr.franz.catcher.Client;

public class TweetLogger {

    public static final Logger log = LoggerFactory.getLogger(TweetLogger.class);

    static final String QUERY_FILE = "query";
    static String queryFilePath = "/tmp/www/in";
    static final Long QUERY_FILE_MONITOR_INTERVAL = 1000l;
    static String queryTerm = "unknown.query";
    static File queryFile;
    static String host = "localhost";
    static int port = 8080;
    static TweetStream ts;

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
                    + "com.mapr.demo.twitter.TweetLogger query_file_path "
                    + "<catcherhost> <catcherport>");
        } else {
            queryFilePath = args[0];
            host = args[1];
            port = Integer.parseInt(args[2]);
            TweetLogger.setQueryFile();
            queryTerm = TweetLogger.getQuery();
            TweetLogger t = new TweetLogger();
            log.info("Invoking filter stream");
            t.startStream();
            TweetLogger.monitorFile(queryFile);
        }
    }

    @SuppressWarnings("unused")
    private void query(Client logger)
            throws IOException, ServiceException, TwitterException {

        // Twitter Client
        Twitter twitter = new TwitterFactory().getInstance();

        // query loop
        try {
            Query query = new Query(queryTerm);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();

                for (Status tweet : tweets) {
                    Tweet.TweetMsg.Builder t = Tweet.TweetMsg.newBuilder();
                    t.setId( tweet.getId() );
                    t.setMsg( tweet.getText() );
                    t.setUser( tweet.getUser().getScreenName() );
                    t.setRt( tweet.getRetweetedStatus().isRetweet() );
                    t.setTime( tweet.getCreatedAt().getTime() );
                    t.setQuery( queryTerm );
                    logger.sendMessage( "tweets", t.build().toByteArray() );

                }
            } while ((query = result.nextQuery()) != null);
        } finally {
            logger.close();
        }

    }

    private class FranzStreamer implements StatusListener {
        private Client c;

        public FranzStreamer(String host, int port)
                throws IOException, ServiceException {
            c = new Client(Lists.newArrayList(new PeerInfo(host, port)));
        }

        public void onStatus(Status s) {
            try {
                Tweet.TweetMsg.Builder t = Tweet.TweetMsg.newBuilder();
                t.setId( s.getId() );
                t.setMsg( s.getText() );
                t.setUser( s.getUser().getScreenName() );
                t.setRt( s.isRetweet() );
                t.setTime( s.getCreatedAt().getTime() );
                t.setQuery( queryTerm );
                c.sendMessage( "tweets", t.build().toByteArray() );
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


    private void startStream() throws IOException, ServiceException {
        StatusListener listener = new FranzStreamer(host, port);
        ts = new TweetStream();
        ts.startStream(listener);
    }

    public static void changeQuery() throws IOException, ServiceException {
        String query = getQuery();
        queryTerm = query;
        log.info("query changing to: " + query);

        // stop stream
    ts.stopStream();

        // restart stream with new term

        ts.changeQuery(queryTerm);

    }

    private static void setQueryFile() throws IOException {

        File f = new File(queryFilePath, QUERY_FILE);
        try {
            Files.createParentDirs(f);
        } catch (IOException e) {
            e.printStackTrace();
        }
        queryFile = f;
    }

    private static String getQuery() throws IOException {
        log.info("Getting query from " + queryFilePath + "/" + QUERY_FILE);
        FileInputStream stream = new FileInputStream(queryFile);
        FileChannel fc = stream.getChannel();
        MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        /* Instead of using default, pass in a decoder. */
        String q = Charset.defaultCharset().decode(bb).toString();
        log.info("Got query: '" + q + "'");
        stream.close();
        return q;
    }

    private static void monitorFile(File file) throws FileNotFoundException {
        FileMonitor monitor = FileMonitor.getInstance();
        monitor.addFileChangeListener(new QueryFileChangeListener(),
                file,
                QUERY_FILE_MONITOR_INTERVAL);
    }
}
