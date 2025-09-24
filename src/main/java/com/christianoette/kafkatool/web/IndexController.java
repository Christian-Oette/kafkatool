package com.christianoette.kafkatool.web;

import com.christianoette.kafkatool.service.TopicReadService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@Controller
public class IndexController {

    private final TopicReadService topicReadService;

    public IndexController(TopicReadService topicReadService) {
        this.topicReadService = topicReadService;
    }

    @GetMapping("/")
    public String index(Model model) {
        List<String> topics = topicReadService.getAllTopicNames();
        model.addAttribute("title", "Kafkatool");
        model.addAttribute("topics", topics);
        return "index";
    }
}
