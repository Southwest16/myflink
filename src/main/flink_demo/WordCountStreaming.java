package flink_examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountStreaming {
    public static void main(String[] args) throws Exception {
        int port;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. Use default port 9000");
            port = 9000;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "";
        String delimiter = "\n";

        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<String, Integer>> windowCounts = text.flatMap(
                (String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split("\\s");

                    for (String word : splits) {
                        out.collect(new Tuple2<>(word, 1));
                    }
        })
            .keyBy("word")
            .timeWindow(Time.seconds(2), Time.seconds(1))
            .sum("count");

        windowCounts.print().setParallelism(2);
        env.execute("Socket window count");
    }

}
