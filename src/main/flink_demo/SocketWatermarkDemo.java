package flink_demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.time.Duration;

public class SocketWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 9000)
                /*.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                );*/
                /*.assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<String>() {
                            private Long currentTimeStamp = 0L;
                            private Long maxOutOfOrderness = 3000L;
                            @Nullable
                            @Override
                            public Watermark getCurrentWatermark() {
                                return new Watermark(currentTimeStamp - maxOutOfOrderness);
                            }

                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                long timeStamp = Long.parseLong(element.split(",")[1]);
                                currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                                return timeStamp;
                            }
                        }
                );*/
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                        return lastElement.startsWith("punctuated_") ? new Watermark(extractedTimestamp) : null;
                    }

                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                });

//        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long> map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new Tuple2<>(split[0], Long.parseLong(split[1]));
//            }
//        })
//                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//                .minBy(1)
//                .print();

        env.execute("watermark demo");
    }
}
