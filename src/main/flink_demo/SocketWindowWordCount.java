package flink_demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


public class SocketWindowWordCount {
	public static void main(String[] args) throws Exception {
		final String hostname;
		final int port;

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
					"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
					"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
					"type the input text into the command line");
			return;
		}

		//get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//启用Checkpoint机制(单位是ms)
		env.enableCheckpointing(1000);
		//设置模式为Exactly-once(这时默认值)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		//checkpoint之间的最小间隔
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		//checkpoint必须在指定时间内完成, 超出这个时间将被丢弃
		env.getCheckpointConfig().setCheckpointTimeout(60000);
		//同一时间只允许操作一个检查点
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		//表示一旦Flink程序被取消后, 会保留Checkpoint数据, 以便根据实际需要恢复到指定的Checkpoint
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		//设置要使用的状态后端
		env.setStateBackend(new RocksDBStateBackend("uri", true));
		//设置重启策略
//		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
		env.setRestartStrategy(RestartStrategies.noRestart());

		//修改时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		//parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text.flatMap(
				(FlatMapFunction<String, WordWithCount>) (s, collector) -> {
					for (String word : s.split("\\s")) {
						collector.collect(new WordWithCount(word, 1L));
					}
				})
				.keyBy("word")
				.timeWindow(Time.seconds(5))
				.reduce(
						(ReduceFunction<WordWithCount>) (wordWithCount, t1) ->
								new WordWithCount(wordWithCount.word, wordWithCount.count + t1.count)
				);


		//print the results with a single thread,rather than in parallel
		windowCounts.print().setParallelism(1);
		env.execute("Socket Window WordCountStreaming");

	}

	public static class WordWithCount {
		public String word;
		public long count;


		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
