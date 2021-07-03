package flink_kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.regex.Pattern;

public class FlinkKafkaDemo {
    /**
     * Flink生产数据到Kafka
     * @throws Exception
     */
    public void kafkaSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**CheckPoint相关配置*/
        //启用CheckPoint, 每5s一次, 并提供EXACTLY_ONCE语义
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //CheckPoint的完成时间, 超出这个时间被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置同一时间点最大并发CheckPoint数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //Externalized checkpoints会在job failed或suspended之后把元数据持久化到外部存储,
        //在这种情况下, 必须要手动清理checkpoint state, meta data和实际的program state.
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /** StateBackend配置 */
        env.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/checkpoint", true));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(300L);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> text = env.socketTextStream("hostname", 9000, "\n");
        //KeyedStream keyedStream = text.keyBy(value -> value.split(", ")[0]);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "broker",
                "topic",
                new SimpleStringSchema()
        );

        text.addSink(producer);
        env.execute("kafka sink");
    }

    /**
     * Flink消费Kafka数据
     * @throws Exception
     */
    public void kafkaSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host01:9092");
        properties.setProperty("group.id", "1");

        //指定一个topic
//        String topic = "topic";
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        //动态读取多个topic中的数据
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(Pattern.compile("topic-[0-5]"),
                new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();
        DataStreamSource<String> ds = env.addSource(consumer);
        ds.print().setParallelism(2);

        env.execute("kafka source");
    }
}
