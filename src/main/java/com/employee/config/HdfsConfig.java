package com.employee.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;

@Configuration
public class HdfsConfig {

    @Value("${hdfs.uri:hdfs://localhost:9000}")
    private String hdfsUri;

    @Value("${hdfs.user:dr.who}")
    private String hdfsUser;

    @Bean
    public FileSystem fileSystem() throws IOException, URISyntaxException, InterruptedException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        return FileSystem.get(new java.net.URI(hdfsUri), configuration, hdfsUser);
    }

}
