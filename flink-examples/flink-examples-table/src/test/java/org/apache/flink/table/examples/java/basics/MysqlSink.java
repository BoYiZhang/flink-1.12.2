package org.apache.flink.table.examples.java.basics;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.DriverManager;
import java.sql.ResultSet;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;



public class MysqlSink extends RichSinkFunction<Tuple3<String,String,String>> {

    private static final long serialVersionUID = -8930276689109741501L;

    private Connection connect = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connect = (Connection) DriverManager.getConnection("jdbc:mysql://192.168.xxx.xxx:3306", "root", "xxxxx");
        ps = (PreparedStatement) connect.prepareStatement("insert into user (id,name,sex) values (?,?,?)");
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        ps.setString(1, value.f0);
        ps.setString(2, value.f1);
        ps.setString(3, value.f2);
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (connect != null) {
                connect.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
