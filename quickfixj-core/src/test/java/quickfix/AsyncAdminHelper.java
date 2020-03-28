package quickfix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.field.SenderCompID;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncAdminHelper implements ApplicationAsyncAdmin {
    private static Logger LOG = LoggerFactory.getLogger(AsyncAdminHelper.class);

    private ExecutorService executor;

    public void start() {
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor();
        }
    }

    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public void beforeAdminSend(final Message message, final SessionID sessionID) {
        start(); // only creates the executor if not already present
        if (executor != null) {
            executor.execute(() -> onMessage(message, sessionID));
        } else {
            throw new IllegalStateException("AsyncAdminHelper not started?");
        }
    }

    private void onMessage(Message fixMessage, SessionID sessionID) {
        try {
            Session session = Session.lookupSession(sessionID);
            if (session == null) {
                LOG.error("Session not found for {}. Ignoring message [{}]", sessionID, fixMessage);
                return;
            }
            if (isOutboundMessage(fixMessage, sessionID)) {
                if (fixMessage.isAsyncAdminEligible()) {
                    LOG.info("##### Sending deferred admin message");
                    session.sendDeferredAdmin(fixMessage);
                } else {
                    LOG.info("##### Sending deferred non-admin message");
                    session.send(fixMessage);
                }
            }
        } catch (FieldNotFound | InvalidMessage | IOException e) {
            throw new RuntimeException(e);
        }
    }

    // this is an outbound message to be sent, if SenderCompID is not set, or it is set and equals our SenderCompID
    private boolean isOutboundMessage(Message fixMessage, SessionID sessionID) throws FieldNotFound {
        Message.Header header = fixMessage.getHeader();
        if (header.isSetField(SenderCompID.FIELD)) {
            return header.getString(SenderCompID.FIELD).equals(sessionID.getSenderCompID());
        } else {
            return true;
        }
    }
}
