package flink_examples;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", numLines);
            }

            @Override
            public String map(String value) throws Exception {
                numLines.add(1);
                return value;
            }
        }).setParallelism(8);

        result.writeAsText("E:\\counter");
        JobExecutionResult jobExecutionResult = env.execute("counter");
        int num= jobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println(num);
    }
}
