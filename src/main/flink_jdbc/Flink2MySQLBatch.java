package flink_jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class Flink2MySQLBatch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //数据库信息
        String driverName = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://host:3306/db_name";
        String username = "user_name";
        String password = "password";

        //SQL
        String selectSQL = "select email_receive_time,raw_data from table";
        String insertSQL = "insert into table_new (email_receive_time,transaction_time,transaction_amount) VALUES (?, ?, ?)";

        //字段类型信息
        TypeInformation[] fieldsTypes = new TypeInformation[]{
                BasicTypeInfo.DATE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldsTypes);

        // JDBC Source
        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbURL)
                .setUsername(username)
                .setPassword(password)
                .setQuery(selectSQL)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        //读取MySQL
        DataSource<Row> dataSource = env.createInput(inputFormat);

        //数据处理
        DataSet ds = dataSource.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                String emailReceiveTime = value.getField(0).toString();
                String rawData = value.getField(1).toString();
                JSONObject json = JSON.parseObject(rawData);

                return Row.of(json.getString("transaction_time"), json.getBigDecimal("transaction_amount"));
            }
        });

        //JDBC Sink
        JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbURL)
                .setUsername(username)
                .setPassword(password)
                .setQuery(insertSQL)
                .finish();
        //写入MySQL
        ds.output(outputFormat);

        env.execute("Flink-MySQL batch");
    }
}
