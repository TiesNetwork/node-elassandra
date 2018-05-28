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
package network.tiesdb.handler.impl.v0r0.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.ebml.TiesDBRequestConsistency;
import com.tiesdb.protocol.v0r0.ebml.format.TiesDBRequestConsistencyFormat;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.end;

import java.util.LinkedList;

import network.tiesdb.handler.impl.v0r0.controller.EntryController.Entry;
import network.tiesdb.service.scope.api.TiesServiceScopeException;

public class ModificationRequestController implements Controller<ModificationRequestController.ModificationRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(ModificationRequestController.class);

    public static class ModificationRequest implements Request {

        private TiesDBRequestConsistency consistency;
        private LinkedList<Entry> entries = new LinkedList<>();

        @Override
        public String toString() {
            return "ModificationRequest [consistency=" + consistency + ", entries=" + entries + "]";
        }

        public TiesDBRequestConsistency getConsistency() {
            return consistency;
        }

        public LinkedList<Entry> getEntries() {
            return entries;
        }

        @Override
        public void accept(Visitor v) throws TiesServiceScopeException {
            v.on(this);
        }

    }

    private final EntryController entryController;

    public ModificationRequestController() {
        this.entryController = new EntryController();
    }

    public boolean acceptModificationRequest(Conversation session, Event e, ModificationRequest r) throws TiesDBProtocolException {
        switch (e.getType()) {
        case CONSISTENCY:
            r.consistency = session.read(TiesDBRequestConsistencyFormat.INSTANCE);
            LOG.debug("CONSISTENCY : {}", r.consistency);
            end(session, e);
            return true;
        case ENTRY:
            Entry entry = new Entry();
            boolean result = entryController.accept(session, e, entry);
            if (result) {
                r.entries.add(entry);
            }
            return result;
        // $CASES-OMITTED$
        default:
            return false;
        }
    }

    @Override
    public boolean accept(Conversation session, Event e, ModificationRequest r) throws TiesDBProtocolException {
        acceptEach(session, e, this::acceptModificationRequest, r);
        return true;
    }

}