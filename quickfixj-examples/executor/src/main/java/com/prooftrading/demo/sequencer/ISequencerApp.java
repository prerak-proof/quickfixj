package com.prooftrading.demo.sequencer;

import com.prooftrading.demo.message.IDemoMessage;

public interface ISequencerApp {
    void setSequencerWriter(SequencerWriter sequencerWriter);

    void stop();

    void onSequencedMessage(IDemoMessage message, boolean isRecovery);

    void start();
}
