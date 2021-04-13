package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.DriverManager;
import java.sql.ResultSet;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;


public class MysqlSource extends RichSourceFunction<Tuple3<String,String,String>> {

    private static final long serialVersionUID = 3334654984018091675L;

    private Connection connect = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connect = (Connection) DriverManager.getConnection("jdbc:mysql://192.168.xx.xx:3306", "root", "xxxxx");
        ps = (PreparedStatement) connect.prepareStatement("select id,name,age from user ");
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, String>> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>();
            tuple.setFields(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
            ctx.collect(tuple);
        }

    }

    @Override
    public void cancel() {
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
