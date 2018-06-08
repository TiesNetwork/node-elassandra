/**
 * Copyright © 2017 Ties BV
 *
 * This file is part of Ties.DB project.
 *
 * Ties.DB project is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Ties.DB project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with Ties.DB project. If not, see <https://www.gnu.org/licenses/lgpl-3.0>.
 */
package network.tiesdb.handler.impl.v0r0.controller.request;

import java.util.concurrent.atomic.AtomicReference;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.controller.Controller;
import network.tiesdb.handler.impl.v0r0.controller.request.ModificationRequestController.ModificationRequest;

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
