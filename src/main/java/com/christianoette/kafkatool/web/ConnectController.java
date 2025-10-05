package com.christianoette.kafkatool.web;

import com.christianoette.kafkatool.service.TopicReadService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class ConnectController {

    private final TopicReadService topicReadService;

    public ConnectController(TopicReadService topicReadService) {
        this.topicReadService = topicReadService;
    }

    @PostMapping("/connect")
    public ResponseEntity<Map<String, Object>> connect() {
        boolean started = topicReadService.connect();
        return ResponseEntity.ok(Map.of(
                "connected", topicReadService.isRunning(),
                "started", started
        ));
    }

    @PostMapping("/disconnect")
    public ResponseEntity<Map<String, Object>> disconnect() {
        topicReadService.disconnect();
        return ResponseEntity.ok(Map.of(
                "connected", topicReadService.isRunning()
        ));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "connected", topicReadService.isRunning()
        ));
    }
}
