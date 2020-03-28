package com.prooftrading.demo.message;

import java.time.Instant;

public class MessageFactory {
    private MessageFactory() {}

    public static IDemoMessage deserialize(String message) {
        String[] parts = message.split("\\|", 5);
        if (parts.length < 5) {
            throw new IllegalArgumentException("not a valid message");
        }
        Instant seqTime = Instant.parse(parts[0]);
        int seqNum = Integer.parseInt(parts[1]);
        int source = Integer.parseInt(parts[2]);
        String msgType = parts[3];
        String payload = parts[4];

        if (DemoFixMessage.MESSAGE_TYPE.equals(msgType)) {
            return new DemoFixMessage(seqTime, seqNum, source, msgType, payload);
        } else if (DemoHBMessage.MESSAGE_TYPE.equals(msgType)) {
            return new DemoHBMessage(seqTime, seqNum, source, msgType, payload);
        }
        return new DemoMessage(seqTime, seqNum, source, msgType, payload);
    }

    public static String serialize(IDemoMessage message) {
        return String.format("%s|%d|%d|%s|%s",
                message.getSeqTime().toString(), message.getSeqNum(), message.getSource(),
                message.getMsgType(), message.getPayload());
    }
}
