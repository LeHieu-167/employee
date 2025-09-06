package com.employee.controller;

import com.employee.dto.EmployeeDto;
import com.employee.service.EmployeeService;
import com.employee.service.StorageService;
import com.employee.service.KafkaRestProducerService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/employees")
public class EmployeeController {
    @Autowired
    private EmployeeService employeeService;

    @Autowired
    private StorageService storageService;

    @Autowired
    private KafkaRestProducerService kafkaRestProducerService;

    @GetMapping("/{id}")
    public ResponseEntity<EmployeeDto> getEmployee(@PathVariable Long id) {
        return ResponseEntity.ok(employeeService.getEmployee(id));
    }

    @GetMapping("/all")
    public ResponseEntity<List<EmployeeDto>> getAllEmployees() {
        return ResponseEntity.ok(employeeService.getAllEmployees());
    }

    @PostMapping
    public ResponseEntity<?> createEmployee(@Valid @RequestBody EmployeeDto employeeDto,
                                            BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            return getValidationErrorResponse(bindingResult);
        }
        return ResponseEntity.ok(employeeService.createEmployee(employeeDto));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateEmployee(@PathVariable Long id,
                                            @Valid @RequestBody EmployeeDto employeeDto,
                                            BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            return getValidationErrorResponse(bindingResult);
        }
        return ResponseEntity.ok(employeeService.updateEmployee(id, employeeDto));
    }

    private ResponseEntity<Map<String, String>> getValidationErrorResponse(BindingResult bindingResult) {
        Map<String, String> errors = new HashMap<>();
        bindingResult.getFieldErrors().forEach(error ->
                errors.put(error.getField(), error.getDefaultMessage())
        );
        return ResponseEntity.badRequest().body(errors);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteEmployee(@PathVariable Long id) {
        employeeService.deleteEmployee(id);
        return ResponseEntity.ok().build();
    }

    // Example endpoint to send a custom message via REST Proxy
    @PostMapping("/send-rest")
    public ResponseEntity<?> sendViaRest(@RequestBody Map<String, String> request) {
        try {
            String message = request.get("message");
            if (message == null || message.trim().isEmpty()) {
                return ResponseEntity.badRequest().body("Message cannot be empty");
        }
            kafkaRestProducerService.sendMessage(message);
            return ResponseEntity.ok("Sent via Kafka REST Proxy");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed to send via Kafka REST Proxy: " + e.getMessage());
        }
    }
}