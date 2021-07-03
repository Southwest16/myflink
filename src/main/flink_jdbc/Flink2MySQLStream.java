package flink_jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class Flink2MySQLStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldsTypes = new TypeInformation[]{
                BasicTypeInfo.DATE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldsTypes);

        String driverName = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://host:3306/db_name";
        String username = "user_name";
        String password = "password";
        String selectSQL = "select email_id,raw_data from bank_statement";
        String insertSQL = "insert into bank_statement_new (email_id,raw_data_json) " +
                "VALUES (?, ?)";

        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbURL)
                .setUsername(username)
                .setPassword(password)
                .setQuery(selectSQL)
                .setRowTypeInfo(rowTypeInfo)
                .finish();


        DataStream ds = env.createInput(inputFormat).map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                String emailId = value.getField(0).toString();
                String rawData = value.getField(1).toString();
                JSONObject json = JSON.parseObject(rawData);

                return Row.of(emailId, json);
            }
        });


        JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbURL)
                .setUsername(username)
                .setPassword(password)
                .setQuery(insertSQL)
                .finish();


        env.execute("Flink connect MySQL streaming");
    }
}
