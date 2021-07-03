package main.flink_project.HotItems;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class HotItems {
	public static void main(String[] args) throws Exception {
		//创建执行流程序的上下文
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//按照EventTime处理
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//获取文件路径
		URL fileURL = HotItems.class.getClassLoader().getResource("");
		Path filePath = Path.fromLocalFile(new File(fileURL.toURI()));

		//抽取UserBehavior的TypeInfo, 是一个PojoTypeInfo
		PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		//由于Java反射抽取出的字段顺序是不确定的, 需要显式指定文件中字段的顺序
		String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};

		//创建PojoCsvInputFormat
		PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);


		env.createInput(csvInput, pojoTypeInfo) //创建数据源
			.assignTimestampsAndWatermarks( //生成watermark
					new AscendingTimestampExtractor<UserBehavior>() {
						@Override
						public long extractAscendingTimestamp(UserBehavior elements) {
							return elements.timestamp * 1000;
						}
					}
			)
			.filter((FilterFunction<UserBehavior>) value -> value.behavior.equals("pv"))
			.keyBy("itemId")
			.timeWindow(Time.minutes(60), Time.seconds(5))
			.aggregate(new CountAgg(), new WindowResultFunction())
			.keyBy("windowEnd")
			.process(new TopNHotItems(5))
			.print();

		env.execute("");
	}

	public static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String> {
		private final int topSize;


		public TopNHotItems(int topSize) {
			this.topSize = topSize;
		}

		//存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发TopN计算
		private ListState<ItemViewCount> itemState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
			itemState = getRuntimeContext().getListState(itemsStateDesc);

		}

		@Override
		public void processElement(
				ItemViewCount input,
				Context context,
				Collector<String> collector) throws Exception {

			// 每条数据都保存到状态中
			itemState.add(input);
			// 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
			context.timerService().registerEventTimeTimer(input.windowEnd + 1);
		}

		@Override
		public void onTimer(
				long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// 获取收到的所有商品点击量
			List<ItemViewCount> allItems = new ArrayList<>();
			for (ItemViewCount item : itemState.get()) {
				allItems.add(item);
			}

			// 提前清除状态中的数据，释放空间
			itemState.clear();
			// 按照点击量从大到小排序
			allItems.sort(((o1, o2) -> (int) (o2.viewCount - o1.viewCount)));

			// 将排名信息格式化成 String, 便于打印
			StringBuilder result = new StringBuilder();
			result.append("====================================\n");
			result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
			for (int i=0; i<allItems.size() && i < topSize; i++) {
				ItemViewCount currentItem = allItems.get(i);
				// No1:  商品ID=12224  浏览量=2413
				result.append("No").append(i).append(":")
						.append("  商品ID=").append(currentItem.itemId)
						.append("  浏览量=").append(currentItem.viewCount)
						.append("\n");
			}
			result.append("====================================\n\n");

			// 控制输出频率，模拟实时滚动结果
			Thread.sleep(1000);

			out.collect(result.toString());
		}
	}

	/**
	 * 用于输出窗口的结果
	 */
	public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

		@Override
		public void apply(
				Tuple tuple,
				TimeWindow window,
				Iterable<Long> input,
				Collector<ItemViewCount> out
		) throws Exception {
			Long itemId = ((Tuple1<Long>) tuple).f0;
			Long count = input.iterator().next();
			out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
		}
	}

	/**
	 * COUNT 统计的聚合函数实现，每出现一条记录加一
	 */
	public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(UserBehavior value, Long accumulator) {
			return accumulator + 1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	/**
	 * 商品点击量
	 */
	public static class ItemViewCount {
		public long itemId;
		public long windowEnd;
		public long viewCount;

		public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
			ItemViewCount result = new ItemViewCount();
			result.itemId = itemId;
			result.windowEnd = windowEnd;
			result.viewCount = viewCount;
			return result;
		}
	}

	/**
	 * 用户行为数据结构
	 */
	public static class UserBehavior {
		public long userId;
		public long itemId;
		public int categoryId;
		public String behavior;
		public long timestamp;
	}
}
