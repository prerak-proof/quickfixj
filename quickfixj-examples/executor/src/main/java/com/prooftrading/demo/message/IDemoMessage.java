package com.prooftrading.demo.message;

import java.time.Instant;

public interface IDemoMessage {
    void sequence(Instant seqTime, int seqNumber);

    Instant getSeqTime();

    int getSeqNum();

    int getSource();

    void setSource(int source);

    String getMsgType();

    String getPayload();
}
