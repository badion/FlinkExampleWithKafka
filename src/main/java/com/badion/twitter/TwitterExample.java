package com.badion.twitter;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;


public class TwitterExample {

    public static final Logger logger = Logger.getLogger(TwitterExample.class);

    public static final String nonAlphaNumeric = "[^a-zA-Z\\d\\s:]";

    private static Properties initTweeterKeys() {
        Properties props = new Properties();
        try {
            props.load(new FileReader(new File("keys.properties")));
            props.setProperty(TwitterSource.CONSUMER_KEY, props.get("CONSUMER_KEY").toString());
            props.setProperty(TwitterSource.CONSUMER_SECRET, props.get("CONSUMER_SECRET").toString());
            props.setProperty(TwitterSource.TOKEN, props.get("TOKEN").toString());
            props.setProperty(TwitterSource.TOKEN_SECRET, props.get("TOKEN_SECRET").toString());
        } catch (IOException e) {
            logger.warn("File not found.");
        }
        return props;
    }

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = initTweeterKeys();
        DataStream<String> streamSource = env.addSource(new TwitterSource(props));
        DataStreamSink<Tuple3<String, Integer, String>> tweets = streamSource
                .filter(new FilterFunction<String>() {
                        @Override
                        public boolean filter(String s) throws Exception {
                            ObjectMapper jsonParser = null;
                            if(jsonParser == null) {
                                jsonParser = new ObjectMapper();
                            }
                            JsonNode jsonNode = jsonParser.readValue(s, JsonNode.class);
                            boolean hasText = jsonNode.has("text");
                            if(hasText) {
                                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").toString());
                                while (tokenizer.hasMoreTokens()) {
                                    String result = tokenizer.nextToken().replaceAll(nonAlphaNumeric, "").toLowerCase();
                                    if(result.contains("clinton") || result.contains("trump")) {
                                        return true;
                                    }
                                }
                            }
                            return false;
                        }
                    }).flatMap(new SelectEnglishAndTokenizeFlatMap())
                .keyBy(0)
                .sum(1)
                .print();

		env.execute("Tweets example");
	}

    public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple3<String, Integer,String>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;
        @Override
        public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) throws Exception {
            if(jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").toString().equals("\"en\"");
            if (isEnglish) {
                    String result = jsonNode.get("user").get("location").toString().toLowerCase();
                    if (!result.equals("") && !result.equals(null) && !result.equals("null")) {
                        out.collect(new Tuple3<String, Integer, String >(jsonNode.get("text").toString(), 1, result));
                        System.out.println(new Tuple3<String, Integer, String >(jsonNode.get("text").toString(), 1, result));
                    }
            }
        }
    }
}
