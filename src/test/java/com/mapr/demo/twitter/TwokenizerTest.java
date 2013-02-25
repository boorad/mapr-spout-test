package com.mapr.demo.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

public class TwokenizerTest {

    @Test
    public void stopWords() {
        Twokenizer t = new Twokenizer();
        assertTrue(t.stopWord("the"));

        List<String> tokens = t.twokenize("the oscars");
        assertTrue(tokens.size() == 1);
    }

    @Test
    public void retweets() {
        Twokenizer t = new Twokenizer();
        assertTrue(t.stopWord("rt"));

        List<String> tokens = t.twokenize("RT @boorad blah");
        assertTrue(tokens.size() == 2);
    }

}
