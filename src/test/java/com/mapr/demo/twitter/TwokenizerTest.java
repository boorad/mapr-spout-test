package com.mapr.demo.twitter;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwokenizerTest {

    private static final Logger log = LoggerFactory.getLogger(TwokenizerTest.class);

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

    @Test
    public void contractions() {
        Twokenizer t = new Twokenizer();
        List<String> tokens = t.twokenize("I'm i'm can't Bob's");
        assertTrue(tokens.size() == 1);
    }

    @Test
    public void punctuation() {
        Twokenizer t = new Twokenizer();
        List<String> tokens = t.twokenize("... !! !!!!!! ( ) ' -");
        for( String token : tokens ) log.debug(token);
        assertTrue(tokens.size() == 0);
    }


}
