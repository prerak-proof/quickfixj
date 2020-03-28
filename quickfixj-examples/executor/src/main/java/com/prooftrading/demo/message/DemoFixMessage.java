package com.prooftrading.demo.message;

import java.time.Instant;

public class DemoFixMessage extends DemoMessage {
    static String MESSAGE_TYPE = "F";

    private String sessionId;
    private String fixMessage;

    public DemoFixMessage(Instant seqTime, int seqNum, int source, String msgType, String payload) {
        super(seqTime, seqNum, source, msgType, payload);
        if (!MESSAGE_TYPE.equals(msgType)) {
            throw new IllegalArgumentException("MsgType must be " + MESSAGE_TYPE);
        }
        this.parsePayload();
    }

    public DemoFixMessage() {
        this("", "");
    }

    public DemoFixMessage(String sessionId, String fixMessage) {
        super(MESSAGE_TYPE, genPayloadString(sessionId, fixMessage));
        this.setFields(sessionId, fixMessage);
    }

    public void init(String sessionId, String fixMessage) {
        this.setFields(sessionId, fixMessage);
        this.setPayload(genPayloadString(sessionId, fixMessage));
    }

    private static String genPayloadString(String sessionId, String fixMessage) {
        return String.format("%s|%s", sessionId, fixMessage);
    }

    private void parsePayload() {
        String[] parts = this.getPayload().split("\\|", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("payload must be sessionId|fixMessage");
        }
        this.setFields(parts[0], parts[1]);
    }

    private void setFields(String sessionId, String fixMessage) {
        this.sessionId = sessionId;
        this.fixMessage = fixMessage;
    }

    public String getSessionID() {
        return sessionId;
    }

    public String getFixMessage() {
        return fixMessage;
    }
}
