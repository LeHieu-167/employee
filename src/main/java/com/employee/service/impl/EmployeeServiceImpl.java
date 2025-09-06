package com.employee.service.impl;

import com.employee.dto.EmployeeDto;
import com.employee.entity.Employee;
import com.employee.repository.EmployeeRepository;
import com.employee.service.EmployeeService;
import com.employee.service.KafkaRestProducerService;
import com.employee.service.SimpleKafkaConnectProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class EmployeeServiceImpl implements EmployeeService {
    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private KafkaRestProducerService kafkaRestProducerService;

    @Autowired
    private SimpleKafkaConnectProducerService kafkaConnectProducerService;

    @Override
    public EmployeeDto createEmployee(EmployeeDto employeeDto) {
        Employee employee = new Employee();
        employee.setName(employeeDto.getName());
        employee.setEmail(employeeDto.getEmail());

        Employee savedEmployee = employeeRepository.save(employee);
        // Gửi qua Kafka REST Proxy (legacy)
        kafkaRestProducerService.sendEmployeeEntity(savedEmployee);
        
        // Gửi structured event qua Kafka Connect để lưu vào HDFS
        kafkaConnectProducerService.sendEmployeeCreateEvent(savedEmployee);

        return convertToDto(savedEmployee);
    }

    @Override
    public EmployeeDto getEmployee(Long id) {
        Employee employee = employeeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Employee not found"));
        return convertToDto(employee);
    }

    @Override
    public List<EmployeeDto> getAllEmployees() {
        return employeeRepository.findAll().stream()
                .map(this::convertToDto)
                .collect(Collectors.toList());
    }

    @Override
    public EmployeeDto updateEmployee(Long id, EmployeeDto employeeDto) {
        Employee employee = employeeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Employee not found"));

        employee.setName(employeeDto.getName());
        employee.setEmail(employeeDto.getEmail());

        Employee updatedEmployee = employeeRepository.save(employee);
        // Gửi qua Kafka REST Proxy (legacy)
        kafkaRestProducerService.sendEmployeeEntity(updatedEmployee);
        
        // Gửi structured event qua Kafka Connect để lưu vào HDFS
        kafkaConnectProducerService.sendEmployeeUpdateEvent(updatedEmployee);

        return convertToDto(updatedEmployee);
    }

    @Override
    public void deleteEmployee(Long id) {
        Employee employee = employeeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Employee not found"));
        employeeRepository.delete(employee);
        // Gửi thông tin xóa qua Kafka REST Proxy (legacy)
        kafkaRestProducerService.sendMessage("DELETE_EMPLOYEE:" + id);
        
        // Gửi structured delete event qua Kafka Connect để lưu vào HDFS
        kafkaConnectProducerService.sendEmployeeDeleteEvent(id);
    }

    private EmployeeDto convertToDto(Employee employee) {
        EmployeeDto dto = new EmployeeDto();
        dto.setId(employee.getId());
        dto.setName(employee.getName());
        dto.setEmail(employee.getEmail());
        return dto;
    }
} 