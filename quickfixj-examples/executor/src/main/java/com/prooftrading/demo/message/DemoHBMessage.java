package com.prooftrading.demo.message;

import java.time.Instant;

public class DemoHBMessage extends DemoMessage {
    static String MESSAGE_TYPE = "H";

    public DemoHBMessage() {
        this(String.valueOf(System.currentTimeMillis()));
    }

    private DemoHBMessage(String payload) {
        super(MESSAGE_TYPE, payload);
    }

    DemoHBMessage(Instant seqTime, int seqNum, int source, String msgType, String payload) {
        super(seqTime, seqNum, source, msgType, payload);
        if (!MESSAGE_TYPE.equals(msgType)) {
            throw new IllegalArgumentException("MsgType must be " + MESSAGE_TYPE);
        }
    }
}
