package com.prooftrading.demo.apps;

import com.prooftrading.demo.message.DemoFixMessage;
import com.prooftrading.demo.message.DemoMessage;
import com.prooftrading.demo.message.IDemoMessage;
import com.prooftrading.demo.sequencer.ISequencerApp;
import com.prooftrading.demo.sequencer.SequencerWriter;
import org.quickfixj.jmx.JmxExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ApplicationAsyncAdmin;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FieldConvertError;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.IncorrectTagValue;
import quickfix.InvalidMessage;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgSeqNum;
import quickfix.field.PossDupFlag;
import quickfix.field.TargetCompID;
import quickfix.fix42.BusinessMessageReject;
import quickfix.fix42.NewOrderSingle;

import javax.management.JMException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientGateway extends quickfix.MessageCracker implements quickfix.Application, ApplicationAsyncAdmin, ISequencerApp {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final SessionSettings settings;
    private SequencerWriter sequencerWriter;

    private SocketAcceptor acceptor;
    private Map<SessionID, Session> sessions;

    public ClientGateway(SessionSettings settings) throws ConfigError, FieldConvertError, IOException, JMException {
        this.settings = settings;
        this.init();
    }

    private void init() throws ConfigError, FieldConvertError, IOException, JMException {
        MessageStoreFactory messageStoreFactory = new FileStoreFactory(this.settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();

        this.acceptor = new SocketAcceptor(this, messageStoreFactory, this.settings, logFactory,
                messageFactory);
        this.acceptor.createSessions();
        this.sessions = getSessions(this.acceptor);

        // reset sessions for outbound messages
        for (Session session : this.sessions.values()) {
            int expectedTargetNum = session.getExpectedTargetNum();
            session.reset();
            session.getStore().setNextTargetMsgSeqNum(expectedTargetNum);
            session.addStateListener(new SessionResetSequencer(session.getSessionID()));
        }

        JmxExporter jmxExporter = new JmxExporter();
        ObjectName connectorObjectName = jmxExporter.register(acceptor);
        log.info("Acceptor registered with JMX, name={}", connectorObjectName);
    }

    private Map<SessionID, Session> getSessions(SocketAcceptor acceptor) {
        Map<SessionID, Session> sessionMap = new HashMap<>();
        List<Session> sessionList = acceptor.getManagedSessions();
        for (Session session : sessionList) {
            sessionMap.put(session.getSessionID(), session);
        }
        return Collections.unmodifiableMap(sessionMap);
    }

    @Override
    public void setSequencerWriter(SequencerWriter sequencerWriter) {
        this.sequencerWriter = sequencerWriter;
    }

    public void start() {
        try {
            this.acceptor.start();
        } catch (ConfigError configError) {
            log.error("error starting acceptor", configError);
            throw new RuntimeException(configError);
        }
    }

    @Override
    public void stop() {
        this.acceptor.stop(true);
    }

    public void onCreate(SessionID sessionID) {
    }

    public void onLogon(SessionID sessionID) {
    }

    public void onLogout(SessionID sessionID) {
    }

    public void toAdmin(Message message, SessionID sessionID) {
    }

    public void toApp(Message message, SessionID sessionID) {
    }

    public void fromAdmin(Message message, SessionID sessionID) {
    }

    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectTagValue, UnsupportedMessageType {
        crack(message, sessionID);
    }

    @SuppressWarnings("unused")
    public void onMessage(NewOrderSingle order, SessionID sessionID) {
        if (isPossDupe(order)) {
            log.info("Ignoring poss dupe order {}", order);
            return;
        }

        sequenceMessage(order, sessionID);
    }

    @SuppressWarnings("unused")
    public void onMessage(BusinessMessageReject businessMessageReject, SessionID sessionID) {
        log.info("Received business reject from session [{}], message: [{}]", sessionID, businessMessageReject);
    }

    private boolean isPossDupe(Message message) {
        Message.Header h = message.getHeader();
        if (h.isSetField(PossDupFlag.FIELD)) {
            try {
                return h.getBoolean(PossDupFlag.FIELD);
            } catch (FieldNotFound fieldNotFound) {
                // ignore
            }
        }
        return false;
    }

    @Override
    public void beforeAdminSend(Message adminMessage, SessionID sessionID) {
        sequenceMessage(adminMessage, sessionID);
    }

    public class SessionResetSequencer extends SessionStateListenerAdapter {
        private final SessionID sessionID;

        public SessionResetSequencer(SessionID sessionID) {
            this.sessionID = sessionID;
        }

        @Override
        public void onReset() {
            DemoMessage dm = new DemoMessage("SR", this.sessionID.toString());
            sequencerWriter.addUnsequenced(dm);
        }
    }

    private void sequenceMessage(Message message, SessionID sessionID) {
        DemoFixMessage dfm = new DemoFixMessage();
        dfm.init(sessionID.toString(), message.toString());
        sequencerWriter.addUnsequenced(dfm);
    }

    private Map<SessionID, Integer> lastTargetSeqNumsSeen = new HashMap<>();

    @Override
    public void onSequencedMessage(IDemoMessage message, boolean isRecovery) {
        try {
            if (!isRecovery && !lastTargetSeqNumsSeen.isEmpty()) {
                log.info("Applying last seen session numbers: " + lastTargetSeqNumsSeen);
                for (Map.Entry<SessionID, Integer> kv : lastTargetSeqNumsSeen.entrySet()) {
                    Integer msgSeqNum = kv.getValue();
                    if (msgSeqNum != null && msgSeqNum > 0) {
                        Session session = this.sessions.get(kv.getKey());
                        if (session != null) {
                            msgSeqNum++;
                            session.getStore().setNextTargetMsgSeqNum(msgSeqNum);
                            log.info("Set stream-recovered target msgSeqNum for session: {}", session);
                        }
                    }
                }
                lastTargetSeqNumsSeen.clear();
            }

            if (message instanceof DemoFixMessage) {
                DemoFixMessage dfm = (DemoFixMessage) message;
                SessionID sessionID = new SessionID(dfm.getSessionID());
                Session session = this.sessions.get(sessionID);
                if (session == null) {
                    log.error("Session not found for " + sessionID);
                    return;
                }

                Message fixMessage = new Message(dfm.getFixMessage());
                boolean isInboundMessage = isInboundMessage(fixMessage, sessionID);

                if (isInboundMessage) {
                    if (isRecovery) {
                        int msgSeqNum = fixMessage.getHeader().getInt(MsgSeqNum.FIELD);
                        this.lastTargetSeqNumsSeen.put(sessionID, msgSeqNum);
                    }
                } else {
                    // outbound async admin eligible message
                    if (fixMessage.isAsyncAdminEligible()) {
                        log.info("Sending deferred admin message: " + fixMessage);
                        session.sendDeferredAdmin(fixMessage);
                    } else {
                        // regular outbound message
                        log.info("Sending outbound message: " + fixMessage);
                        session.send(fixMessage);
                    }
                }
            } else {
                if ("SR".equals(message.getMsgType())) {
                    SessionID sessionID = new SessionID(message.getPayload());
                    Session session = this.sessions.get(sessionID);
                    if (session == null) {
                        log.error("Session not found for " + sessionID);
                        return;
                    }
                    // only reset session based on this message when it is not connected, because a connected session
                    // would have performed this reset already
                    if (!session.hasResponder()) {
                        log.info("Resetting session seq numbers: {}", session);
                        session.reset();
                    }
                }
            }

        } catch (FieldNotFound | InvalidMessage | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isInboundMessage(Message message, SessionID sessionID) throws FieldNotFound {
        Message.Header header = message.getHeader();
        if (!header.isSetField(TargetCompID.FIELD)) return false;
        String targetCompID = header.getString(TargetCompID.FIELD);
        return sessionID.getSenderCompID().equals(targetCompID);
    }
}
