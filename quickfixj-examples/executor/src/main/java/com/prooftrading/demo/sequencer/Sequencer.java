package com.prooftrading.demo.sequencer;

import com.prooftrading.demo.message.DemoFixMessage;
import com.prooftrading.demo.message.DemoHBMessage;
import com.prooftrading.demo.message.IDemoMessage;
import com.prooftrading.demo.message.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Sequencer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Sequencer.class);

    private static final long HB_INTERVAL_NANOS = 1_000_000_000L;
    private static final String SEQUENCER_FILE = "target/data/sequencer/file.txt";
    private static final int SEQUENCER_SOURCE = 0;

    private final Deque<IDemoMessage> unsequenced = new LinkedBlockingDeque<>();
    private final List<IDemoMessage> sequenced = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger sequence = new AtomicInteger(0);

    private Thread sequencer = new Thread(this);
    private RandomAccessFile sequencerFile;

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    public Sequencer() throws IOException {
        this.sequencer.setDaemon(true);

        initSequencerFile();
        recoverFromSequencerFile();
    }

    private void initSequencerFile() throws FileNotFoundException {
        File f = new File(SEQUENCER_FILE);
        File p = f.getParentFile();
        if (!p.exists() && !p.mkdirs()) {
            throw new IllegalStateException("Unable to create parent directory " + p);
        }
        this.sequencerFile = new RandomAccessFile(f, "rw");
    }

    private void recoverFromSequencerFile() throws IOException {
        // load existing stream
        String line;
        while ((line = this.sequencerFile.readLine()) != null) {
            cacheSequenced(MessageFactory.deserialize(line));
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public boolean start() {
        if (!isStarted.compareAndSet(false, true)) {
            return false;
        }
        if (!sequencer.isAlive()) {
            sequencer.start();
        }
        return true;
    }

    @SuppressWarnings("unused")
    public boolean stop() {
        isStarted.set(false);
        if (sequencer.isAlive()) {
            sequencer.interrupt();
            try {
                sequencer.join(1000);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting to stop sequencer", e);
            }
            if (sequencer.isAlive()) {
                LOG.warn("Unable to kill sequencer thread cleanly");
                return false;
            }
        }
        return true;
    }

    @Override
    public void run() {
        long lastHBTime = 0;
        while (isStarted.get()) {
            long currentTime = System.nanoTime();
            try {
                if (currentTime - lastHBTime > HB_INTERVAL_NANOS) {
                    produceSequenced(genHeartbeat());
                    lastHBTime = currentTime;
                }
            } catch (IOException e) {
                LOG.info("Error producing sequenced message");
            }
            try {
                IDemoMessage r = unsequenced.poll();
                if (r == null) {
                    Thread.sleep(100);
                } else {
                    produceSequenced(r);
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
            } catch (IOException e) {
                LOG.error("Unable to process unsequenced record", e);
            }
        }
    }

    private void produceSequenced(IDemoMessage record) throws IOException {
        int seq = this.sequence.incrementAndGet();
        record.sequence(Instant.now(), seq);
        LOG.info("Producing sequenced: {}", record);
        addToFile(record);
        cacheSequenced(record);
    }

    private void cacheSequenced(IDemoMessage record) {
        int seqNum = record.getSeqNum();
        sequenced.add(seqNum - 1, record);
        if (seqNum > sequence.get()) {
            sequence.set(seqNum);
        }
    }

    private void addToFile(IDemoMessage line) throws IOException {
        sequencerFile.writeBytes(line + "\n");
    }

    void addUnsequenced(IDemoMessage record) {
        unsequenced.add(record);
    }

    IDemoMessage getSequenced(int seqNum) {
        int index = seqNum - 1;
        if (index >= sequenced.size()) {
            return null;
        }
        return sequenced.get(index);
    }

    public int getSequenceCount() {
        return sequenced.size();
    }

    private IDemoMessage genHeartbeat() {
        IDemoMessage dm = new DemoHBMessage();
        dm.setSource(SEQUENCER_SOURCE);
        return dm;
    }

    public SequencerWriter getWriter() {
        return new SequencerWriter(this);
    }

    public SequencerReader getReader() {
        return new SequencerReader(this);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Sequencer s = new Sequencer();
        s.start();

        SequencerWriter sc = s.getWriter();
        for (int i = 0; i < 5; i++) {
            String is = String.valueOf(i);
            LOG.info("Offering to unsequenced: {}", is);
            sc.addUnsequenced(new DemoFixMessage("sessionId", "fixMessage" + i));
            Thread.sleep(500);
        }

        SequencerReader sr = s.getReader();
        int count = 1;
        while (count < 1000) {
            IDemoMessage r = sr.nextSequenced();
            if (r == null) continue;
            count++;
            LOG.info("Found sequenced record: {}", r);
        }
    }
}
