package quickfix;

/**
 * The purpose of this interface is to allow the Application to have full control over messages sent directly by
 * the library, either in response to an incoming message or due to a timer event.
 */
public interface ApplicationAsyncAdmin {
    /**
     * @param adminMessage The admin message that library wishes to send
     * @param sessionID The session for which the admin message is being sent
     */
    void beforeAdminSend(Message adminMessage, SessionID sessionID);
}
