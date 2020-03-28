package com.prooftrading.demo.message;

import java.time.Instant;

public class DemoMessage implements IDemoMessage {
    private Instant seqTime;
    private int seqNum;
    private int source;
    private String msgType;
    private String payload;

    public DemoMessage(String msgType, String payload) {
        this(Instant.now(), 0, 0, msgType, payload);
    }

    DemoMessage(Instant seqTime, int seqNum, int source, String msgType, String payload) {
        this.sequence(seqTime, seqNum);
        this.setSource(source);
        this.setMsgType(msgType);
        this.setPayload(payload);
    }

    @Override
    public void sequence(Instant seqTime, int seqNumber) {
        this.seqTime = seqTime;
        this.seqNum = seqNumber;
    }

    @Override
    public Instant getSeqTime() {
        return seqTime;
    }

    @Override
    public int getSeqNum() {
        return seqNum;
    }

    @Override
    public int getSource() {
        return source;
    }

    @Override
    public void setSource(int source) {
        this.source = source;
    }

    @Override
    public String getMsgType() {
        return msgType;
    }

    private void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return MessageFactory.serialize(this);
    }
}
