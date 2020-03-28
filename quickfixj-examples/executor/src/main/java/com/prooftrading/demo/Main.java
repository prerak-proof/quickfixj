package com.prooftrading.demo;

import com.prooftrading.demo.apps.ClientGateway;
import com.prooftrading.demo.apps.OMS;
import com.prooftrading.demo.message.IDemoMessage;
import com.prooftrading.demo.sequencer.ISequencerApp;
import com.prooftrading.demo.sequencer.Sequencer;
import com.prooftrading.demo.sequencer.SequencerReader;
import com.prooftrading.demo.sequencer.SequencerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ConfigError;
import quickfix.FieldConvertError;
import quickfix.SessionSettings;

import javax.management.JMException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private final static Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main(String[] cgConfigs) throws IOException, ConfigError, FieldConvertError, JMException {
        List<ISequencerApp> apps = new ArrayList<>();
        List<SequencerWriter> writers = new ArrayList<>();

        Sequencer sequencer = new Sequencer();
        int sequenceCount = sequencer.getSequenceCount();
        sequencer.start();

        // create CGs
        for (String cgConfig : cgConfigs) {
            InputStream inputStream = Main.class.getResourceAsStream(cgConfig);
            SessionSettings settings = new SessionSettings(inputStream);
            inputStream.close();

            SequencerWriter sw = sequencer.getWriter();
            ClientGateway cg = new ClientGateway(settings);
            cg.setSequencerWriter(sw);
            writers.add(sw);
            apps.add(cg);
        }

        // create OMS
        OMS oms = new OMS();
        SequencerWriter omsSW = sequencer.getWriter();
        oms.setSequencerWriter(omsSW);
        writers.add(omsSW);
        apps.add(oms);

        // catch up on stream
        SequencerReader sr = sequencer.getReader();
        for (int i = 1; i <= sequenceCount; i++) {
            IDemoMessage message = sr.getSequenced(i);
            if (message == null) {
                break;
            }
            for (ISequencerApp app : apps) {
                try {
                    app.onSequencedMessage(message, true);
                } catch (Exception e) {
                    LOG.error("App error on sequenced message during recovery", e);
                }
            }
        }

        LOG.info("###########################  RECOVERY COMPLETE  ###############################");

        for (SequencerWriter sw : writers) {
            sw.onRecoveryComplete();
        }

        // start them all
        for (ISequencerApp app : apps) {
            app.start();
        }

        // ongoing messages
        while (true) {
            IDemoMessage message = sr.nextSequenced();
            if (message == null) {
                break;
            }
            for (ISequencerApp app : apps) {
                try {
                    app.onSequencedMessage(message, false);
                } catch (Exception e) {
                    LOG.error("App error on sequenced message", e);
                }
            }
        }
    }

    public static void main(String[] args) {
        String[] executorConfigs = new String[]{"clgw1.cfg"};
        try {
            new Main(executorConfigs);
        } catch (IOException | ConfigError | FieldConvertError | JMException e) {
            LOG.error("Error in main", e);
        }
    }
}
