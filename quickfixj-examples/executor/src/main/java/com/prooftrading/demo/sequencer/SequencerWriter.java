package com.prooftrading.demo.sequencer;

import com.prooftrading.demo.message.IDemoMessage;

import java.util.concurrent.atomic.AtomicInteger;

public class SequencerWriter {
    private static final AtomicInteger sources = new AtomicInteger(0);

    private final int source = sources.incrementAndGet();

    private final Sequencer sequencer;

    private boolean isRecoveryComplete = false;

    SequencerWriter(Sequencer sequencer) {
        this.sequencer = sequencer;
    }

    public void addUnsequenced(IDemoMessage record) {
        // ignore messages produced during recovery
        if (!isRecoveryComplete) {
            return;
        }
        record.setSource(this.source);
        this.sequencer.addUnsequenced(record);
    }

    public void onRecoveryComplete() {
        isRecoveryComplete = true;
    }

    public int getSource() {
        return source;
    }
}
