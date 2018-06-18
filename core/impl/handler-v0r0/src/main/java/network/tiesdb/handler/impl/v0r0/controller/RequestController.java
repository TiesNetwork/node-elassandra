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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBException;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;
import com.tiesdb.protocol.v0r0.ebml.format.UUIDFormat;
import com.tiesdb.protocol.v0r0.reader.ComputeRetrieveReader.ComputeRetrieve;
import com.tiesdb.protocol.v0r0.reader.FieldReader.Field;
import com.tiesdb.protocol.v0r0.reader.FieldRetrieveReader.FieldRetrieve;
import com.tiesdb.protocol.v0r0.reader.FilterReader.Filter;
import com.tiesdb.protocol.v0r0.reader.FunctionReader.ArgumentFunction;
import com.tiesdb.protocol.v0r0.reader.FunctionReader.ArgumentReference;
import com.tiesdb.protocol.v0r0.reader.FunctionReader.ArgumentStatic;
import com.tiesdb.protocol.v0r0.reader.FunctionReader.FunctionArgument;
import com.tiesdb.protocol.v0r0.reader.ModificationEntryReader.ModificationEntry;
import com.tiesdb.protocol.v0r0.reader.ModificationRequestReader.ModificationRequest;
import com.tiesdb.protocol.v0r0.reader.Reader.Request;
import com.tiesdb.protocol.v0r0.reader.RecollectionRequestReader.RecollectionRequest;
import com.tiesdb.protocol.v0r0.reader.RecollectionRequestReader.Retrieve;
import com.tiesdb.protocol.v0r0.reader.RequestReader;
import com.tiesdb.protocol.v0r0.writer.ModificationErrorWriter.ModificationErrorResult;
import com.tiesdb.protocol.v0r0.writer.ModificationResponseWriter.ModificationResponse;
import com.tiesdb.protocol.v0r0.writer.ModificationResponseWriter.ModificationResult;
import com.tiesdb.protocol.v0r0.writer.ModificationSuccessWriter.ModificationSuccessResult;
import com.tiesdb.protocol.v0r0.writer.ResponseWriter;
import com.tiesdb.protocol.v0r0.writer.Writer.Response;

import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeQuery;
import network.tiesdb.service.scope.api.TiesServiceScopeQuery.Query;
import network.tiesdb.type.Duration;
import one.utopic.sparse.ebml.format.ASCIIStringFormat;
import one.utopic.sparse.ebml.format.BigDecimalFormat;
import one.utopic.sparse.ebml.format.BigIntegerFormat;
import one.utopic.sparse.ebml.format.BytesFormat;
import one.utopic.sparse.ebml.format.DateFormat;
import one.utopic.sparse.ebml.format.DoubleFormat;
import one.utopic.sparse.ebml.format.FloatFormat;
import one.utopic.sparse.ebml.format.IntegerFormat;
import one.utopic.sparse.ebml.format.LongFormat;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

public class RequestController implements Request.Visitor<Response> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestController.class);

    private static class TiesServiceScopeActionEntry implements TiesServiceScopeAction {

        private final ModificationEntry modificationEntry;
        private final Map<String, TiesValue> fieldValues;

        public TiesServiceScopeActionEntry(ModificationEntry modificationEntry) throws TiesServiceScopeException {
            this.modificationEntry = modificationEntry;
            this.fieldValues = new HashMap<>();
            for (Map.Entry<String, Field> e : modificationEntry.getFields().entrySet()) {
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
            return modificationEntry.getHeader().getTablespaceName();
        }

        @Override
        public String getTableName() {
            return modificationEntry.getHeader().getTableName();
        }

        @Override
        public byte[] getHeaderRawBytes() {
            return modificationEntry.getHeader().getRawBytes();
        }

        @Override
        public Map<String, TiesValue> getFieldValues() {
            return fieldValues;
        }

        @Override
        public long getEntryVersion() {
            return modificationEntry.getHeader().getEntryVersion();
        }
    }

    private static Object deserialize(Field field) throws TiesServiceScopeException {
        return getFormatForType(field.getType()).apply(field.getRawValue());
    }

    private static Function<byte[], ?> getFormatForType(String type) throws TiesServiceScopeException {
        switch (type) {
        case "int":
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
        case "bigint":
            return BigIntegerFormat.INSTANCE::readFormat;
        case "string":
            return UTF8StringFormat.INSTANCE::readFormat;
        case "ascii":
            return ASCIIStringFormat.INSTANCE::readFormat;
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
            throw new TiesServiceScopeException("Unknown type " + type);
        }
    }

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
        for (ModificationEntry modificationEntry : request.getEntries()) {

            try {
                TiesServiceScopeAction action = new TiesServiceScopeActionEntry(modificationEntry);

                switch (modificationEntry.getHeader().getEntryType()) {
                case ENTRY_TYPE_INSERT:
                    serviceScope.insert(action);
                    break;
                case ENTRY_TYPE_UPDATE:
                    serviceScope.update(action);
                    break;
                default:
                    throw new TiesServiceScopeException("Unknown EntryType " + modificationEntry.getHeader().getEntryType());
                }

            } catch (TiesServiceScopeException e) {
                LOG.error("Error handling ModificationRequest.Entry {}", modificationEntry, e);
                results.add(new ModificationErrorResult() {

                    @Override
                    public Throwable getError() {
                        return e;
                    }

                    @Override
                    public byte[] getEntryHeaderHash() {
                        return modificationEntry.getHeader().getHeaderHash();
                    }

                });
                continue;
            }
            results.add(new ModificationSuccessResult() {

                @Override
                public byte[] getEntryHeaderHash() {
                    return modificationEntry.getHeader().getHeaderHash();
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

    private static void convert(List<FunctionArgument> from, Consumer<Query.Function.Argument> c) {
        for (FunctionArgument arg : from) {
            c.accept(arg.accept(new FunctionArgument.Visitor<Query.Function.Argument>() {

                @Override
                public Query.Function.Argument on(ArgumentFunction arg) {
                    return new Query.Function.FunctionArgument() {

                        List<Argument> arguments = new LinkedList<>();
                        {
                            convert(arg.getFunction().getArguments(), arguments::add);
                        }

                        @Override
                        public String getName() {
                            return arg.getFunction().getName();
                        }

                        @Override
                        public List<Argument> getArguments() {
                            return arguments;
                        }

                    };
                }

                @Override
                public Query.Function.Argument on(ArgumentReference arg) {
                    return new Query.Function.FieldArgument() {

                        @Override
                        public String getFieldName() {
                            return arg.getFieldName();
                        }

                    };
                }

                @Override
                public Query.Function.Argument on(ArgumentStatic arg) {
                    return new Query.Function.ValueArgument() {

                        @Override
                        public Object getValue() throws TiesServiceScopeException {
                            return getFormatForType(getType()).apply(getRawValue());
                        }

                        @Override
                        public String getType() {
                            return arg.getType();
                        }

                        @Override
                        public byte[] getRawValue() {
                            return arg.getRawValue();
                        }
                    };
                }
            }));
        }
    }

    @Override
    public Response on(RecollectionRequest request) throws TiesDBProtocolException {
        requireNonNull(request);

        TiesServiceScope serviceScope = service.newServiceScope();
        LOG.debug("Service scope: {}", serviceScope);

        try {
            serviceScope.select(new TiesServiceScopeQuery() {

                private final List<Query.Selector> selectors = new LinkedList<>();
                {
                    for (Retrieve r : request.getRetrieves()) {
                        selectors.add(r.accept(new Retrieve.Visitor<Query.Selector>() {

                            @Override
                            public Query.Selector on(FieldRetrieve retrieve) {
                                return new Query.Selector.FieldSelector() {
                                    @Override
                                    public String getFieldName() {
                                        return retrieve.getFieldName();
                                    }
                                };
                            }

                            @Override
                            public Query.Selector on(ComputeRetrieve retrieve) {
                                return new Query.Selector.FunctionSelector() {

                                    List<Argument> arguments = new LinkedList<>();
                                    {
                                        convert(retrieve.getArguments(), arguments::add);
                                    }

                                    @Override
                                    public String getName() {
                                        return retrieve.getName();
                                    }

                                    @Override
                                    public List<Argument> getArguments() {
                                        return arguments;
                                    }

                                    @Override
                                    public String getAlias() {
                                        return retrieve.getAlias();
                                    }
                                };
                            }
                        }));
                    }
                }
                private final List<Query.Filter> filters = new LinkedList<>();
                {
                    for (Filter filter : request.getFilters()) {
                        filters.add(new Query.Filter() {

                            private final List<Argument> arguments = new LinkedList<>();
                            {
                                convert(filter.getArguments(), arguments::add);
                            }

                            @Override
                            public String getName() {
                                return filter.getName();
                            }

                            @Override
                            public List<Argument> getArguments() {
                                return arguments;
                            }

                            @Override
                            public String getFieldName() {
                                return filter.getFieldName();
                            }
                        });
                    }
                }

                @Override
                public Query getQuery() {
                    return new Query() {

                        @Override
                        public String getTablespaceName() {
                            return request.getTablespaceName();
                        }

                        @Override
                        public String getTableName() {
                            return request.getTableName();
                        }

                        @Override
                        public List<Selector> getSelectors() {
                            return selectors;
                        }

                        @Override
                        public List<Filter> getFilters() {
                            return filters;
                        }
                    };
                }

            });
            throw new TiesDBProtocolException("Success: " + request.toString());
        } catch (TiesServiceScopeException e) {
            LOG.error("Error handling RecollectionRequest {}", request, e);
            throw new TiesDBProtocolException("Error: " + request.toString());
        }

    }

}
