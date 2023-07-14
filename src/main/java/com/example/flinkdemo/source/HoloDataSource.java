package com.example.flinkdemo.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HoloDataSource extends RichSourceFunction<Tuple2<String, Long>> {
    private static final Logger logger = LoggerFactory.getLogger(HoloDataSource.class);
    private Connection connection;
    private PreparedStatement psmt;

    boolean running = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        String propertiesFilePath = "C:\\Users\\pengzheng\\IdeaProjects\\flink-demo\\src\\main\\resources\\application-holo.properties";
        File propertiesFile = new File(propertiesFilePath);
        InputStream pfis = Files.newInputStream(propertiesFile.toPath());
        ParameterTool parameter = ParameterTool.fromPropertiesFile(pfis);
        String diverName = parameter.get("driver-class-name");
        String url = parameter.get("url");
        String username = parameter.get("username");
        String password = parameter.get("password");
        connection = DriverManager.getConnection(url, username, password);
        String sql = "select to_char(to_timestamp(receive_time/1000),'YYYY-MM-DD HH24:MI') as receive_minute,event_time from prod_sdk_data_cn_ba where to_timestamp(receive_time/1000) >=( now() - INTERVAL '12 hour')";
        psmt = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (running) {
            ResultSet resultSet = psmt.executeQuery();
            while (resultSet.next()) {
                Long eventTime = resultSet.getLong("event_time");
                String receiveMinute = resultSet.getString("receive_minute");
                ctx.collect(new Tuple2<String, Long>(receiveMinute, eventTime));
            }
        }
//        while (resultSet.next()){
//            resultSet.getString("")
//        }
    }

    @Override
    public void close() throws Exception {
        cancel();
        psmt.close();
        connection.close();
        logger.info("holo data source closed");
    }

    @Override
    public void cancel() {
        running = false;
    }
}
