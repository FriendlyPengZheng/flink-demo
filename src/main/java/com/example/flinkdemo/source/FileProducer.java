package com.example.flinkdemo.source;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@Slf4j
public class FileProducer {
    public static void main(String[] args) throws IOException {
        int fileSize = 1024 * 1024 * 1024 * 1; // 文件大小为 1G
        int lineSize = 100; // 每行长度为 100 个字符
        int uniqueLineCount = 6543; // 不重复行数为 500

        String fileName = "data.txt";
        // 生成不重复行
        Set<String> uniqueLines = new HashSet<>();
        while (uniqueLines.size() < uniqueLineCount) {
            String line = generateRandomLine(lineSize);
            uniqueLines.add(line);
        }

        // 将不重复行写入文件中
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String line : uniqueLines) {
                writer.write(line);
                writer.newLine();
            }
        }

        // 将重复行写入文件中
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            Random rand = new Random();
            int repeatLineCount = (fileSize - uniqueLineCount * lineSize) / lineSize;
            for (int i = 0; i < repeatLineCount; i++) {
                String line = uniqueLines.stream().skip(rand.nextInt(uniqueLineCount)).findFirst().orElse("");
                writer.write(line);
                writer.newLine();
            }
        }
        log.info("file init finished，文件行数为：{}", getFileLineCount(fileName));
    }

    private static String generateRandomLine(int length) {
        StringBuilder sb = new StringBuilder(length);
        Random rand = new Random();
        for (int i = 0; i < length; i++) {
            sb.append((char) (rand.nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    public static int getFileLineCount(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int lineCount = 0;
            while (reader.readLine() != null) {
                lineCount++;
            }
            return lineCount;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
