package com.mapr.demo.storm.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.mapr.demo.storm.TweetTopology;

public class JSONWriter {

    static final Logger log = LoggerFactory.getLogger(JSONWriter.class);
    static Properties props = TweetTopology.loadProperties();;

    public static void write(JSONObject json, String type) {
        try {
            String dir = props.getProperty("doc.root");
            String file = props.getProperty(type);
            //log.debug(dir + "/" + file);

            // write to temp copy and rename to get atomic effect
            File outputFile = new File(dir, file);
            File tmp = new File(dir, file + ".tmp");
            if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
                throw new RuntimeException(String.format("Cannot create output directory %s", outputFile.getParentFile()));
            }

            Files.write(JSONValue.toJSONString(json), tmp, Charset.forName("UTF-8"));
            if (!tmp.renameTo(outputFile)) {
                log.error("Unable to overwrite {} using rename", outputFile);
                throw new IOException("Cannot overwrite data file");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
