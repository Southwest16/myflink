package flink_examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchDemoBroadcast {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1. 准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("Flink", 1));
        broadData.add(new Tuple2<>("Spark", 2));
        broadData.add(new Tuple2<>("Kafka", 3));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        //处理需要广播的数据, 把数据集转换成Map类型, map中的key是用户姓名, value是用户年龄
        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(
                new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        HashMap<String, Integer> res = new HashMap<>();
                        res.put(value.f0, value.f1);
                        return res;
                    }});

        //源数据
        DataSource<String> data = env.fromElements("Flink", "Spark", "Kafka");

        //注意: 这里使用RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<>();

            //这个方法只会执行一次, 可以在这里实现一些初始化的功能,
            //因此可以再open方法中获取广播变量数据
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2. 获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadcastMapName");
                for (HashMap<String, Integer> map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + " ---> " + age;
            }
        }).withBroadcastSet(toBroadcast, "broadcastMapName");

        result.print();
    }
}
