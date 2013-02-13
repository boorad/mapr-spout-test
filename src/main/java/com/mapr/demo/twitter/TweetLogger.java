package com.mapr.demo.twitter;

import java.io.IOException;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.Client;

public class TweetLogger {

    /**
     * @param args
     * @throws ServiceException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ServiceException {

        query(args[0]);
//        stream(args[0]);
    }

    private static void query(String query_term) throws IOException, ServiceException {

        // Franz client
        Client c = new Client(
                Lists.newArrayList(new PeerInfo("localhost", 8080)));

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
        } finally {
            c.close();
            System.exit(0);
        }

    }

    // TODO: try streaming API
//    public static void stream(String query_term) {}

}
