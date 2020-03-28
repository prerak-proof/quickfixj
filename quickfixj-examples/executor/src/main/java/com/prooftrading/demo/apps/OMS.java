package com.prooftrading.demo.apps;

import com.prooftrading.demo.message.DemoFixMessage;
import com.prooftrading.demo.message.IDemoMessage;
import com.prooftrading.demo.sequencer.ISequencerApp;
import com.prooftrading.demo.sequencer.SequencerWriter;
import quickfix.DataDictionary;
import quickfix.DefaultDataDictionaryProvider;
import quickfix.FieldNotFound;
import quickfix.FixVersions;
import quickfix.InvalidMessage;
import quickfix.Message;
import quickfix.MessageUtils;
import quickfix.field.AvgPx;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastShares;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrderQty2;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.NewOrderSingle;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OMS implements ISequencerApp {
    private static final int SESSION_ID_TAG = 99999;
    private static final int FILL_SIZE = 1;
    private static final int FILL_INTERVAL_MILLIS = 1000;

    private final DataDictionary dd = new DefaultDataDictionaryProvider().getSessionDataDictionary(FixVersions.BEGINSTRING_FIX42);
    private int orderId;
    private int execId;

    private List<NewOrderSingle> orders = new ArrayList<>();
    private SequencerWriter sequenceWriter;

    @Override
    public void onSequencedMessage(IDemoMessage message, boolean isRecovery) {
        try {
            if (message instanceof DemoFixMessage) {
                onFIXMessage((DemoFixMessage) message, isRecovery);
            }
            doTime(message);
        } catch (InvalidMessage | FieldNotFound e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setSequencerWriter(SequencerWriter sequencerWriter) {
        this.sequenceWriter = sequencerWriter;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    private Instant lastFillTime = Instant.EPOCH;

    private void doTime(IDemoMessage message) throws FieldNotFound {
        Instant currentTime = message.getSeqTime();
        if (Duration.between(lastFillTime, currentTime).toMillis() <= FILL_INTERVAL_MILLIS) {
            return;
        }
        lastFillTime = currentTime;

        Iterator<NewOrderSingle> itr = orders.iterator();
        while (itr.hasNext()) {
            NewOrderSingle order = itr.next();
            if (fillOrder(order)) {
                itr.remove();
            }
        }
    }

    private void onFIXMessage(DemoFixMessage fixOrder, boolean isRecovery) throws InvalidMessage, FieldNotFound {
        String fixMessage = fixOrder.getFixMessage();
        String msgType = MessageUtils.getStringField(fixMessage, MsgType.FIELD);
        if (msgType == null) { return; }
        if ("D".equals(msgType)) {
            NewOrderSingle nos = new NewOrderSingle();
            nos.fromString(fixMessage, dd, true);
            nos.setDouble(OrderQty2.FIELD, nos.getOrderQty().getValue());
            nos.setString(SESSION_ID_TAG, fixOrder.getSessionID());
            orders.add(nos);
            ackOrder(nos);
        }
    }

    private void ackOrder(NewOrderSingle nos) throws FieldNotFound {
        ExecutionReport accept = new ExecutionReport(genOrderID(), genExecID(),
                new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.NEW), new OrdStatus(OrdStatus.NEW),
                nos.getSymbol(), nos.getSide(), new LeavesQty(nos.getOrderQty().getValue()), new CumQty(0), new AvgPx(0));
        accept.set(nos.getClOrdID());
        sendMessage(nos.getString(SESSION_ID_TAG), accept);
    }

    private boolean fillOrder(NewOrderSingle nos) throws FieldNotFound {
        int origOrderQty = nos.getInt(OrderQty2.FIELD);
        int leaves = nos.getInt(OrderQty.FIELD);

        char execType = ExecType.PARTIAL_FILL;
        char ordStatus = OrdStatus.PARTIALLY_FILLED;

        // do the execution
        int lastShares = Math.min(leaves, FILL_SIZE);
        leaves = leaves - lastShares;
        int cumQty = origOrderQty - leaves;
        double px = getPrice(nos);
        nos.setInt(OrderQty.FIELD, leaves);

        if (leaves == 0) {
            execType = ExecType.FILL;
            ordStatus = OrdStatus.FILLED;
        }

        ExecutionReport fill = new ExecutionReport(genOrderID(), genExecID(),
                new ExecTransType(ExecTransType.NEW), new ExecType(execType), new OrdStatus(ordStatus),
                nos.getSymbol(), nos.getSide(), new LeavesQty(leaves), new CumQty(cumQty), new AvgPx(px));
        fill.set(nos.getClOrdID());
        fill.setDouble(OrderQty.FIELD, origOrderQty);
        fill.set(new LastShares(lastShares));
        fill.set(new LastPx(px));

        sendMessage(nos.getString(SESSION_ID_TAG), fill);

        return leaves == 0;
    }

    private double getPrice(NewOrderSingle nos) throws FieldNotFound {
        if (nos.isSetPrice()) {
            return nos.getPrice().getValue();
        }
        return 10.0;  // no limit set, fill at $10
    }

    private void sendMessage(String sessionID, Message message) {
        DemoFixMessage dfm = new DemoFixMessage();
        dfm.init(sessionID, message.toString());
        this.sequenceWriter.addUnsequenced(dfm);
    }

    private OrderID genOrderID() {
        return new OrderID(String.valueOf(++orderId));
    }

    private ExecID genExecID() {
        return new ExecID(String.valueOf(++execId));
    }
}
