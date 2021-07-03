package flink_demo.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    private final int threshold;
    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //清空前一次checkpoint状态中的值
        checkpointedState.clear();
        for (Tuple2<String, Integer> e : bufferedElements) {
            //向状态中添加缓存元素
            checkpointedState.add(e);
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //operator state的初始化和keyed state是很相似的。同样是通过初始化一个StateDescriptor实例。
        ListStateDescriptor<Tuple2<String, Integer>> desc = new ListStateDescriptor<Tuple2<String, Integer>>(
                "buffered-elements", //状态名称
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})); //状态所存储值的类型

        //创建一个list state
        checkpointedState = context.getOperatorStateStore().getListState(desc);
        //创建一个union list state
        //checkpointedState = context.getOperatorStateStore().getUnionListState(desc);


        if (context.isRestored()) { //状态是否是从之前的快照进行恢复的（比如应用程序崩溃失败之后）
            for (Tuple2<String, Integer> e : checkpointedState.get()) {
                bufferedElements.add(e);
            }
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> e : bufferedElements) {
                //写到sink
            }
            bufferedElements.clear();
        }
    }
}
