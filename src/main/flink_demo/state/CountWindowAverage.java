package flink_demo.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    //Tuple2中第一个是count，第二个是sum
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        //访问状态中的值
        Tuple2<Long, Long> currentSum = sum.value();

        if (currentSum == null) {
            currentSum = Tuple2.of(0L, 0L);
        }

        //更新count
        currentSum.f0 += 1;
        //更新sum
        currentSum.f1 += input.f1;
        //更新状态
        sum.update(currentSum);
        //如果count大于等于2，计算平均值并清除状态
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", //状态名
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}) //类型
                );
        //获取状态句柄
        sum = getRuntimeContext().getState(descriptor);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //内存状态后端
        env.setStateBackend(new MemoryStateBackend(5, false));
        env.setStateBackend(new FsStateBackend("hdfs://...", false));
        env.setStateBackend(new RocksDBStateBackend("file://...", false));

        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();

        // the printed output will be (1,4) and (1,5)
        env.execute("State Demo");
    }
}


