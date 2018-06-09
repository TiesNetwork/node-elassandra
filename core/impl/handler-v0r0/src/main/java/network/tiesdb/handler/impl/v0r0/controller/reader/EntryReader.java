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

import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.checkEntryFieldsHash;

import java.util.HashMap;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;

import network.tiesdb.handler.impl.v0r0.controller.reader.EntryHeaderReader.EntryHeader;
import network.tiesdb.handler.impl.v0r0.controller.reader.FieldReader.Field;

public class EntryReader implements Reader<EntryReader.Entry> {

    public static class Entry {

        private EntryHeader header;
        private HashMap<String, Field> fields = new HashMap<>();

        @Override
        public String toString() {
            return "Entry [header=" + header + ", fields=" + fields + "]";
        }

        public EntryHeader getHeader() {
            return header;
        }

        public HashMap<String, Field> getFields() {
            return fields;
        }

    }

    private final EntryHeaderReader entryHeaderReader;
    private final ListReader<Field> fieldListController;

    public EntryReader() {
        this.entryHeaderReader = new EntryHeaderReader();
        this.fieldListController = new ListReader<>(TiesDBType.FIELD, () -> new Field(), new FieldReader());
    }

    private boolean acceptEntry(Conversation session, Event e, Entry entry) throws TiesDBProtocolException {
        switch (e.getType()) {
        case ENTRY_HEADER:
            EntryHeader header = new EntryHeader();
            boolean result = entryHeaderReader.accept(session, e, header);
            if (result) {
                if (null != entry.header) {
                    throw new TiesDBProtocolException("Multiple headers detected! Should be only one header in each entry.");
                }
                entry.header = header;
            }
            return true;
        case FIELD_LIST:
            return fieldListController.accept(session, e, field -> {
                entry.fields.put(field.getName(), field);
            });
        // $CASES-OMITTED$
        default:
            return false;
        // throw new TiesDBProtocolException("Illegal packet format");
        }
    }

    @Override
    public boolean accept(Conversation session, Event e, Entry entry) throws TiesDBProtocolException {
        acceptEach(session, e, this::acceptEntry, entry);
        if (!checkEntryFieldsHash(entry)) {
            throw new TiesDBProtocolException("Entry fields hash missmatch.");
        }
        return true;
    }

}
