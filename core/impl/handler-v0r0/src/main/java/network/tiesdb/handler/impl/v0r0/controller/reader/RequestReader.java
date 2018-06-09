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
package network.tiesdb.handler.impl.v0r0.controller.reader;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.util.CheckedConsumer;

import network.tiesdb.handler.impl.v0r0.controller.reader.ModificationRequestReader.ModificationRequest;

public class RequestReader implements Reader<CheckedConsumer<Reader.Request, TiesDBProtocolException>> {

    private final ModificationRequestReader modificationRequestReader = new ModificationRequestReader();

    @Override
    public boolean accept(Conversation session, Event e, CheckedConsumer<Reader.Request, TiesDBProtocolException> requestHandler) throws TiesDBProtocolException {
        switch (e.getType()) {
        case MODIFICATION_REQUEST:
            ModificationRequest request = new ModificationRequest();
            boolean result = modificationRequestReader.accept(session, e, request);
            if (result) {
                requestHandler.accept(request);
            }
            return result;
        // $CASES-OMITTED$
        default:
            return false;
        }
    }

}
