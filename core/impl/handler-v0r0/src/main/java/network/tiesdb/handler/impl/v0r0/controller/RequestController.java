package network.tiesdb.handler.impl.v0r0.controller;

import java.util.concurrent.atomic.AtomicReference;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.controller.ModificationRequestController.ModificationRequest;

public class RequestController implements Controller<AtomicReference<Request>> {

    private final ModificationRequestController modificationRequestController;

    public RequestController() {
        this.modificationRequestController = new ModificationRequestController();
    }

    @Override
    public boolean accept(Conversation session, Event e, AtomicReference<Request> requestRef) throws TiesDBProtocolException {
        switch (e.getType()) {
        case MODIFICATION_REQUEST:
            ModificationRequest request = new ModificationRequest();
            boolean result = modificationRequestController.accept(session, e, request);
            if (result) {
                if (!requestRef.compareAndSet(null, request)) {
                    throw new TiesDBProtocolException("Request in progress, can't handle another one.");
                }
            }
            return result;
        // $CASES-OMITTED$
        default:
            return false;
        }
    }

}
