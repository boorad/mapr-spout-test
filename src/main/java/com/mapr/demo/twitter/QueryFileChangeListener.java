package com.mapr.demo.twitter;

import java.io.File;
import java.io.IOException;

import com.google.protobuf.ServiceException;

public class QueryFileChangeListener implements FileChangeListener {

    public void fileChanged(File file) throws IOException {
        try {
            TweetLogger.changeQuery();
        } catch (ServiceException e) {
            e.printStackTrace();
        }
    }

}
