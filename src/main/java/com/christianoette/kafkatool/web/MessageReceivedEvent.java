package com.christianoette.kafkatool.web;

public class MessageReceivedEvent {
    private final String topicName;
    private final String headerData;
    private final String payload;
    private final String timestamp;

    public MessageReceivedEvent(String topicName, String headerData, String payload, String timestamp) {
        this.topicName = topicName;
        this.headerData = headerData;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getHeaderData() {
        return headerData;
    }

    public String getPayload() {
        return payload;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
