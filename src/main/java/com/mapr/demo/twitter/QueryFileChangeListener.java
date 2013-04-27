package com.mapr.demo.twitter;

import java.io.File;
import java.io.IOException;

public class QueryFileChangeListener implements FileChangeListener {

    public void fileChanged(File file) throws IOException {
        TweetLogger.changeQuery();
    }

}
