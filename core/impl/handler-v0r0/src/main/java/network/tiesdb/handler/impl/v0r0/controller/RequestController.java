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

import static com.tiesdb.protocol.v0r0.ebml.TiesDBConstants.ENTRY_TYPE_INSERT;
import static com.tiesdb.protocol.v0r0.ebml.TiesDBConstants.ENTRY_TYPE_UPDATE;
import static java.util.Objects.requireNonNull;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBException;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;
import com.tiesdb.protocol.v0r0.ebml.format.UUIDFormat;

import network.tiesdb.handler.impl.v0r0.controller.reader.EntryReader.Entry;
import network.tiesdb.handler.impl.v0r0.controller.reader.FieldReader.Field;
import network.tiesdb.handler.impl.v0r0.controller.reader.ModificationRequestReader.ModificationRequest;
import network.tiesdb.handler.impl.v0r0.controller.reader.Reader.Request;
import network.tiesdb.handler.impl.v0r0.controller.reader.RequestReader;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationResponseWriter.ModificationResponse;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationResponseWriter.ModificationResult;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationSuccessWriter.ModificationSuccessResult;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationErrorWriter.ModificationErrorResult;
import network.tiesdb.handler.impl.v0r0.controller.writer.ResponseWriter;
import network.tiesdb.handler.impl.v0r0.controller.writer.Writer.Response;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesValue;
import network.tiesdb.type.Duration;
import one.utopic.sparse.ebml.format.BigDecimalFormat;
import one.utopic.sparse.ebml.format.BytesFormat;
import one.utopic.sparse.ebml.format.DateFormat;
import one.utopic.sparse.ebml.format.DoubleFormat;
import one.utopic.sparse.ebml.format.FloatFormat;
import one.utopic.sparse.ebml.format.IntegerFormat;
import one.utopic.sparse.ebml.format.LongFormat;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

public class RequestController implements Request.Visitor<Response> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestController.class);

    private final TiesService service;
    private final RequestReader requestReader;
    private final ResponseWriter responseWriter;

    public RequestController(TiesService service, RequestReader requestReader, ResponseWriter responseWriter) {
        this.service = service;
        this.requestReader = requestReader;
        this.responseWriter = responseWriter;
    }

    public void handle(Conversation session) throws TiesDBException {
        Event event;
        while (null != requestReader && null != (event = session.get())) {
            LOG.debug("RootBeginEvent: {}", event);
            if (EventState.BEGIN.equals(event.getState())) {
                try {
                    if (requestReader.accept(session, event, request -> responseWriter.accept(session, processRequest(request)))) {
                        continue;
                    }
                } catch (TiesDBProtocolException e) {
                    throw new TiesDBException("Request failed", e);
                }
                LOG.warn("Skipped {}", event);
                session.skip();
                Event endEvent = session.get();
                LOG.debug("RootEndEvent: {}", endEvent);
                if (null != endEvent && EventState.END.equals(endEvent.getState()) && endEvent.getType().equals(event.getType())) {
                    continue;
                }
            }
            throw new TiesDBProtocolException("Illegal root event: " + event);
        }
    }

    public Response processRequest(Request request) throws TiesDBProtocolException {
        if (null != request) {
            if (null == request.getMessageId()) {
                throw new TiesDBProtocolException("Request MessageId is required");
            }
            return request.accept(this);
        }
        throw new TiesDBProtocolException("Empty request");
    }

    @Override
    public ModificationResponse on(ModificationRequest request) throws TiesDBProtocolException {
        requireNonNull(request);

        TiesServiceScope serviceScope = service.newServiceScope();
        LOG.debug("Service scope: {}", serviceScope);

        LinkedList<ModificationResult> results = new LinkedList<>();
        for (Entry entry : request.getEntries()) {

            try {
                TiesServiceScopeAction action = new TiesServiceScopeActionEntry(entry);

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

            } catch (TiesServiceScopeException e) {
                LOG.error("Error handling entry {}", entry, e);
                results.add(new ModificationErrorResult() {

                    @Override
                    public Throwable getError() {
                        return e;
                    }

                    @Override
                    public byte[] getEntryHeaderHash() {
                        return entry.getHeader().getHeaderHash();
                    }

                });
                continue;
            }
            results.add(new ModificationSuccessResult() {

                @Override
                public byte[] getEntryHeaderHash() {
                    return entry.getHeader().getHeaderHash();
                }

            });
        }

        return new ModificationResponse() {

            @Override
            public BigInteger getMessageId() {
                return request.getMessageId();
            }

            @Override
            public Iterable<ModificationResult> getResults() {
                return results;
            }

        };
    }

    private static class TiesServiceScopeActionEntry implements TiesServiceScopeAction {

        private final Entry entry;
        private final Map<String, TiesValue> fieldValues;

        public TiesServiceScopeActionEntry(Entry entry) throws TiesServiceScopeException {
            this.entry = entry;
            this.fieldValues = new HashMap<>();
            for (Map.Entry<String, Field> e : entry.getFields().entrySet()) {
                if (null != e.getValue().getRawValue()) {
                    Field field = e.getValue();
                    Object fieldValue = deserialize(field);
                    fieldValues.put(e.getKey(), new TiesValue() {

                        @Override
                        public String getType() {
                            return field.getType();
                        }

                        @Override
                        public byte[] getFieldFullRawBytes() {
                            return field.getRawBytes();
                        }

                        @Override
                        public byte[] getBytes() {
                            return field.getRawValue();
                        }

                        @Override
                        public Object get() {
                            return fieldValue;
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
            return entry.getHeader().getRawBytes();
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

    private static Object deserialize(Field field) throws TiesServiceScopeException {
        return getFormatForType(field.getType()).apply(field.getRawValue());
    }

    private static Function<byte[], ?> getFormatForType(String type) throws TiesServiceScopeException {
        switch (type) {
        case "integer":
            return IntegerFormat.INSTANCE::readFormat;
        case "long":
            return LongFormat.INSTANCE::readFormat;
        case "float":
            return FloatFormat.INSTANCE::readFormat;
        case "double":
            return DoubleFormat.INSTANCE::readFormat;
        case "decimal":
            return BigDecimalFormat.INSTANCE::readFormat;
        case "string":
            return UTF8StringFormat.INSTANCE::readFormat;
        case "binary":
            return BytesFormat.INSTANCE::readFormat;
        case "time":
            return DateFormat.INSTANCE::readFormat;
        case "uuid":
            return UUIDFormat.INSTANCE::readFormat;
        case "boolean":
            return data -> IntegerFormat.INSTANCE.readFormat(data) != 0;
        case "duration":
            return data -> new Duration(BigDecimalFormat.INSTANCE.readFormat(data));
        default:
            throw new TiesServiceScopeException("Unknown field type " + type);
        }
    }

}
