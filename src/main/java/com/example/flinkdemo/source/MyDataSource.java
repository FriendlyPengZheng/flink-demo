package com.example.flinkdemo.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class MyDataSource extends RichSourceFunction<String> {
    volatile boolean isKeepRunning;
    private static final MyDataSource myDataSource = new MyDataSource(true);

    private MyDataSource(boolean isKeepRunning) {
        this.isKeepRunning = isKeepRunning;
    }

    public static synchronized MyDataSource build() {
        return myDataSource;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int fileSize = 1024 * 1024 * 1024 * 1; // 文件大小为 1G
        int lineSize = 100; // 每行长度为 100 个字符
        int uniqueLineCount = 6543; // 不重复行数为 500
        Random rand = new Random();
        // 生成不重复行
        Set<String> uniqueLines = new HashSet<>();
        while (uniqueLines.size() < uniqueLineCount) {
            String line = generateRandomLine(lineSize);
            uniqueLines.add(line);
        }
        while (isKeepRunning) {
            // 将重复行写入文件中
            String line = uniqueLines.stream().skip(rand.nextInt(uniqueLineCount)).findFirst().orElse("");
            Thread.sleep(5L);
            ctx.collectWithTimestamp(line, System.currentTimeMillis() - 1000 * 10L);
        }
    }

    @Override
    public void cancel() {
        isKeepRunning = false;
    }

    private String generateRandomLine(int length) {
        StringBuilder sb = new StringBuilder(length);
        Random rand = new Random();
        for (int i = 0; i < length; i++) {
            sb.append((char) (rand.nextInt(26) + 'a'));
        }
        return sb.toString();
    }

}
