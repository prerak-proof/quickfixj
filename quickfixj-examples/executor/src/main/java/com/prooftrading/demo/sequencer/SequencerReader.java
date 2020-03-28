package com.prooftrading.demo.sequencer;

import com.prooftrading.demo.message.IDemoMessage;

public class SequencerReader {
    private final Sequencer sequencer;

    private int currentSequence = 0;

    SequencerReader(Sequencer sequencer) {
        this.sequencer = sequencer;
    }

    public IDemoMessage getSequenced(int sequence) {
        IDemoMessage message = this.sequencer.getSequenced(sequence);
        if (message != null) {
            this.currentSequence = sequence;
        }
        return message;
    }

    public IDemoMessage nextSequenced() {
        IDemoMessage message;
        while (true) {
            message = this.sequencer.getSequenced(currentSequence + 1);
            if (message == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // someone stopped this thread?
                    break;
                }
            } else {
                currentSequence++;
                break;
            }
        }
        return message;
    }

    public int getSequenceCount() {
        return this.sequencer.getSequenceCount();
    }
}
