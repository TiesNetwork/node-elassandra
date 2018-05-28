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
package network.tiesdb.handler.impl.v0r0.processor;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tiesdb.protocol.v0r0.ebml.TiesDBConstants.*;

import network.tiesdb.handler.impl.v0r0.controller.EntryController.Entry;
import network.tiesdb.handler.impl.v0r0.controller.FieldController.Field;
import network.tiesdb.handler.impl.v0r0.controller.ModificationRequestController.ModificationRequest;
import network.tiesdb.handler.impl.v0r0.controller.Request;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeActionInsert;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesValue;

public class RequestProcessor implements Request.Visitor {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessor.class);

    private final TiesService service;

    public RequestProcessor(TiesService service) {
        this.service = service;
    }

    public void processRequest(Request request) throws TiesServiceScopeException {
        if (null != request) {
            request.accept(this);
            return;
        }

        LOG.error("Empty request");
        return;
    }

    @Override
    public void on(ModificationRequest request) throws TiesServiceScopeException {
        requireNonNull(request);
        TiesServiceScope serviceScope = service.newServiceScope();
        LOG.debug("Service scope: {}", serviceScope);
        for (Entry entry : request.getEntries()) {

            TiesServiceScopeActionInsert action = new TiesServiceScopeActionInsertEntry(entry);

            switch (entry.getHeader().getEntryType()) {
            case ENTRY_TYPE_INSERT:
                serviceScope.insert(action);
                break;
            case ENTRY_TYPE_UPDATE:
                serviceScope.update(action);
                break;
            default:
                throw new TiesServiceScopeException("Unknown EntryType " + entry.getHeader().getEntryType());
            }

        }
    }

    private static class TiesServiceScopeActionInsertEntry implements TiesServiceScopeActionInsert {

        private final Entry entry;
        private final Map<String, TiesValue> fieldValues;

        public TiesServiceScopeActionInsertEntry(Entry entry) {
            this.entry = entry;
            this.fieldValues = new HashMap<>();
            for (Map.Entry<String, Field> e : entry.getFields().entrySet()) {
                if (null != e.getValue().getValue()) {
                    fieldValues.put(e.getKey(), new TiesValue() {

                        @Override
                        public String getType() {
                            return e.getValue().getType();
                        }

                        @Override
                        public byte[] getFieldFullRawBytes() {
                            return new byte[0];
                        }

                        @Override
                        public byte[] getBytes() {
                            return e.getValue().getValue();
                        }
                    });
                }
            }
        }

        @Override
        public String getTablespaceName() {
            return entry.getHeader().getTablespaceName();
        }

        @Override
        public String getTableName() {
            return entry.getHeader().getTableName();
        }

        @Override
        public byte[] getHeaderRawBytes() {
            return new byte[0];
        }

        @Override
        public Map<String, TiesValue> getFieldValues() {
            return fieldValues;
        }

        @Override
        public long getEntryVersion() {
            return entry.getHeader().getEntryVersion();
        }
    }
}
