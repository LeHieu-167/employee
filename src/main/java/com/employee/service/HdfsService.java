package com.employee.service;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HdfsService {
    private static final Logger logger = LoggerFactory.getLogger(HdfsService.class);

    @Autowired
    private FileSystem fileSystem;
    /*
     * Ghi dữ liệu vào HDFS.
     *
     * @param filePath Đường dẫn đến file trên HDFS.
     * @param data Dữ liệu cần ghi.
     */

    public void writeToHdfs(String filePath, String data) {
        Path path = new Path(filePath);
        try (FSDataOutputStream outputStream = fileSystem.create(path)) {
            outputStream.writeUTF(data);
            logger.info("Data written to HDFS at path: {}", filePath);
        } catch (IOException e) {
            logger.error("Failed to write data to HDFS at path {}: {}", filePath, e.getMessage());
        }
    }
}
