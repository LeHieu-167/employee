package com.employee.service.impl;

import com.employee.config.StorageProperties;
import com.employee.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.stream.Stream;

@Service
public class FileSystemStorageService implements StorageService {

    private final Path rootLocation;
    private final KafkaTemplate<String, byte[]> fileKafkaTemplate;

    @Autowired
    public FileSystemStorageService(StorageProperties properties, KafkaTemplate<String, byte[]> fileKafkaTemplate) {
        this.rootLocation = Paths.get(properties.getLocation());
        this.fileKafkaTemplate = fileKafkaTemplate;
        init();
    }

    @Override
    public void init() {
        try {
            Files.createDirectories(rootLocation);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize storage location", e);
        }
    }

    @Override
    public void store(MultipartFile file) {
        try {
            if (file == null) {
                throw new RuntimeException("Failed to store null file");
            }
            if (file.isEmpty()) {
                throw new RuntimeException("Failed to store empty file");
            }
            
            String filename = StringUtils.cleanPath(Objects.requireNonNull(file.getOriginalFilename()));
            if (filename.isEmpty()) {
                throw new RuntimeException("Failed to store file with empty filename");
            }
            if (filename.contains("..")) {
                throw new RuntimeException("Cannot store file with relative path outside current directory");
            }

            // Lưu file vào hệ thống
            try (InputStream inputStream = file.getInputStream()) {
                Files.copy(inputStream, this.rootLocation.resolve(filename),
                        StandardCopyOption.REPLACE_EXISTING);
            }

            // Gửi file qua Kafka
            try {
                fileKafkaTemplate.send("file-topic", filename, file.getBytes());
            } catch (Exception e) {
                // Nếu gửi Kafka thất bại, xóa file đã lưu
                Files.deleteIfExists(this.rootLocation.resolve(filename));
                throw new RuntimeException("Failed to send file to Kafka", e);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to store file", e);
        }
    }

    @Override
    public Stream<Path> loadAll() {
        try {
            return Files.walk(this.rootLocation, 1)
                    .filter(path -> !path.equals(this.rootLocation))
                    .map(this.rootLocation::relativize);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read stored files", e);
        }
    }

    @Override
    public Path load(String filename) {
        return rootLocation.resolve(filename);
    }

    @Override
    public Resource loadAsResource(String filename) {
        try {
            Path file = load(filename);
            Resource resource = new UrlResource(file.toUri());
            if (resource.exists() || resource.isReadable()) {
                return resource;
            } else {
                throw new RuntimeException("Could not read file: " + filename);
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not read file: " + filename, e);
        }
    }

    @Override
    public void deleteAll() {
        try {
            Files.walk(this.rootLocation, 1)
                    .filter(path -> !path.equals(this.rootLocation))
                    .map(Path::toFile)
                    .forEach(file -> file.delete());
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete files", e);
        }
    }
} 