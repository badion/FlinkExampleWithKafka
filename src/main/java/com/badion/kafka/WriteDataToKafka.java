package com.badion.kafka;


import com.badion.twitter.TwitterExample;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Created by cloudera on 11/23/16.
 */
public class WriteDataToKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());
        DataStreamSink<String> topic;
        topic = messageStream.addSink(new FlinkKafkaProducer08<String>(parameterTool.getRequired("bootstrap.servers"),
                parameterTool.getRequired("topic"),
               new SimpleStringSchema()));
        env.execute();
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 0;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                Properties props = TwitterExample.initTweeterKeys();
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                String nonAlphaNumeric = "[^a-zA-Z\\d\\s:]";
                DataStream<String> streamSource = env.addSource(new TwitterSource(props));
                SingleOutputStreamOperator<Tuple3<String, Integer, String>> tweets = streamSource
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
                        }).flatMap(new TwitterExample.SelectEnglishAndTokenizeFlatMap())
                        .keyBy(0)
                        .sum(1);
                env.getConfig().setTaskCancellationInterval(10);

                //** messages passed to kafka
//                ctx.collect();
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
