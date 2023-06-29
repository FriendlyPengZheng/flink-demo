package com.example.flinkdemo;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlinkDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkDemoApplication.class, args);
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        configuration.setString(RestOptions.BIND_PORT, "8081");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
