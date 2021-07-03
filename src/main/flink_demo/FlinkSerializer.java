package flink_examples;

import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkSerializer {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //使用Avro序列化
        env.getConfig().enableForceAvro();

        //使用Kryo序列化
        env.getConfig().enableForceKryo();

        //使用自定义序列化
//        env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass);
    }
}
