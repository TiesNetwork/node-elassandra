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

import static java.util.Objects.requireNonNull;

import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBException;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;
import com.tiesdb.protocol.v0r0.exception.TiesDBProtocolMessageException;
import com.tiesdb.protocol.v0r0.reader.ComputeRetrieveReader.ComputeRetrieve;
import com.tiesdb.protocol.v0r0.reader.EntryHeaderReader.EntryHeader;
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
import com.tiesdb.protocol.v0r0.writer.ModificationResponseWriter.ModificationResponse;
import com.tiesdb.protocol.v0r0.writer.ModificationResponseWriter.ModificationResult;
import com.tiesdb.protocol.v0r0.writer.ModificationResultErrorWriter.ModificationResultError;
import com.tiesdb.protocol.v0r0.writer.ModificationResultSuccessWriter.ModificationResultSuccess;
import com.tiesdb.protocol.v0r0.writer.RecollectionResponseWriter.RecollectionResponse;
import com.tiesdb.protocol.v0r0.writer.RecollectionResultWriter.RecollectionResult;
import com.tiesdb.protocol.v0r0.writer.Writer.Response;

import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesEntryHeader;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeModification;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;

public class RequestController implements Request.Visitor<Response> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestController.class);

    private static final byte[] EMPTY_ARRAY = new byte[0];

    protected static class EntryImpl implements Entry {

        private final ModificationEntry modificationEntry;
        private final Map<String, Entry.FieldValue> fieldValues;
        private final Map<String, Entry.FieldHash> fieldHashes;

        public EntryImpl(ModificationEntry modificationEntry, boolean forInsert) throws TiesServiceScopeException {
            this.modificationEntry = modificationEntry;
            Map<String, Entry.FieldValue> fieldValues = new HashMap<>();
            Map<String, Entry.FieldHash> fieldHashes = new HashMap<>();
            for (Map.Entry<String, Field> e : modificationEntry.getFields().entrySet()) {
                Field field = e.getValue();
                if (null != field.getRawValue()) {
                    Object fieldValue = deserialize(field);
                    fieldValues.put(e.getKey(), new Entry.FieldValue() {

                        @Override
                        public String getType() {
                            return field.getType();
                        }

                        @Override
                        public byte[] getHash() {
                            return field.getHash();
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
                } else if (forInsert) {
                    throw new TiesServiceScopeException("Insert should have only value fields");
                } else {
                    fieldHashes.put(e.getKey(), new Entry.FieldHash() {
                        @Override
                        public byte[] getHash() {
                            return field.getHash();
                        }
                    });
                }
            }
            this.fieldHashes = fieldHashes;
            this.fieldValues = fieldValues;
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
        public Map<String, Entry.FieldHash> getFieldHashes() {
            return fieldHashes;
        }

        @Override
        public Map<String, Entry.FieldValue> getFieldValues() {
            return fieldValues;
        }

        @Override
        public TiesEntryHeader getHeader() {
            EntryHeader header = modificationEntry.getHeader();
            return new TiesEntryHeader() {

                @Override
                public Date getEntryTimestamp() {
                    return header.getEntryTimestamp();
                }

                @Override
                public short getEntryNetwork() {
                    return header.getEntryNetwork().shortValue();
                }

                @Override
                public BigInteger getEntryVersion() {
                    return header.getEntryVersion();
                }

                @Override
                public byte[] getEntryFldHash() {
                    return header.getEntryFldHash();
                }

                @Override
                public byte[] getSigner() {
                    return header.getSigner();
                }

                @Override
                public byte[] getSignature() {
                    return header.getSignature();
                }

                @Override
                public byte[] getHash() {
                    return header.getHash();
                }

                @Override
                public byte[] getEntryOldHash() {
                    return header.getEntryOldHash();
                }
            };
        }
    }

    static Object deserialize(Field field) throws TiesServiceScopeException {
        return ControllerUtil.readerForType(field.getType()).apply(field.getRawValue());
    }

    private final TiesService service;
    private final RequestReader requestReader;
    private final ResponseController responseController;

    public RequestController(TiesService service, RequestReader requestReader, ResponseController responseController) {
        this.service = service;
        this.requestReader = requestReader;
        this.responseController = responseController;
    }

    public void handle(Conversation session) throws TiesDBException {
        Event event;
        while (null != requestReader && null != (event = session.get())) {
            LOG.debug("RootBeginEvent: {}", event);
            if (EventState.BEGIN.equals(event.getState())) {
                if (requestReader.accept(session, event, request -> responseController.handle(session, processRequest(request)))) {
                    continue;
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

    protected Response processRequest(Request request) throws TiesDBProtocolException {
        if (null == request) {
            throw new TiesDBProtocolException("Empty request");
        }
        LOG.debug("Request: {}", request);
        if (null == request.getMessageId()) {
            throw new TiesDBProtocolException("Request MessageId is required");
        }
        try {
            return request.accept(this);
        } catch (TiesDBProtocolMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new TiesDBProtocolMessageException(request.getMessageId(), e);
        }
    }

    @Override
    public ModificationResponse on(ModificationRequest request) throws TiesDBProtocolMessageException {
        requireNonNull(request);

        TiesServiceScope serviceScope = service.newServiceScope();
        LOG.debug("Service scope: {}", serviceScope);

        LinkedList<ModificationResult> results = new LinkedList<>();
        for (ModificationEntry modificationEntry : request.getEntries()) {
            EntryHeader header = modificationEntry.getHeader();
            if (null == header) {
                IllegalArgumentException e = new IllegalArgumentException("No header");
                LOG.error("Error handling ModificationRequest.Entry {}", modificationEntry, e);
                results.add(new ModificationResultError() {

                    @Override
                    public Throwable getError() {
                        return e;
                    }

                    @Override
                    public byte[] getEntryHeaderHash() {
                        return EMPTY_ARRAY;
                    }

                });
                continue;
            }
            try {
                if (null == header.getEntryOldHash() && BigInteger.ONE.equals(header.getEntryVersion())) {
                    serviceScope.insert(new TiesServiceScopeModification() {

                        private final EntryImpl entry = new EntryImpl(modificationEntry, true);

                        @Override
                        public Entry getEntry() {
                            return entry;
                        }

                    });
                } else if(null != header.getEntryOldHash() && BigInteger.ZERO.equals(header.getEntryVersion())){
                    serviceScope.delete(new TiesServiceScopeModification() {

                        private final EntryImpl entry = new EntryImpl(modificationEntry, false);

                        @Override
                        public Entry getEntry() {
                            return entry;
                        }

                    });
                } else if(null != header.getEntryOldHash()) {
                    serviceScope.update(new TiesServiceScopeModification() {

                        private final EntryImpl entry = new EntryImpl(modificationEntry, false);

                        @Override
                        public Entry getEntry() {
                            return entry;
                        }

                    });
                } else {
                    throw new TiesServiceScopeException("Illegal modification EntryOldHash and/or EntryVersion");
                }
            } catch (TiesServiceScopeException e) {
                LOG.error("Error handling ModificationRequest.Entry {}", modificationEntry, e);
                results.add(new ModificationResultError() {

                    @Override
                    public Throwable getError() {
                        return e;
                    }

                    @Override
                    public byte[] getEntryHeaderHash() {
                        return modificationEntry.getHeader().getHash();
                    }

                });
                continue;
            }
            results.add(new ModificationResultSuccess() {

                @Override
                public byte[] getEntryHeaderHash() {
                    return modificationEntry.getHeader().getHash();
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

    protected static void convertArguments(List<FunctionArgument> from, Consumer<Query.Function.Argument> c) {
        for (FunctionArgument arg : from) {
            c.accept(arg.accept(new FunctionArgument.Visitor<Query.Function.Argument>() {

                @Override
                public Query.Function.Argument on(ArgumentFunction arg) {
                    return new Query.Function.Argument.FunctionArgument() {

                        List<Argument> arguments = new LinkedList<>();
                        {
                            convertArguments(arg.getFunction().getArguments(), arguments::add);
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
                    return new Query.Function.Argument.FieldArgument() {

                        @Override
                        public String getFieldName() {
                            return arg.getFieldName();
                        }

                    };
                }

                @Override
                public Query.Function.Argument on(ArgumentStatic arg) {
                    return new Query.Function.Argument.ValueArgument() {

                        @Override
                        public Object getValue() throws TiesServiceScopeException {
                            return ControllerUtil.readerForType(getType()).apply(getRawValue());
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
    public Response on(RecollectionRequest request) throws TiesDBProtocolMessageException {
        requireNonNull(request);

        BigInteger messageId = request.getMessageId();
        LOG.debug("MessageID: {}", messageId);

        TiesServiceScope serviceScope = service.newServiceScope();
        LOG.debug("Service scope: {}", serviceScope);

        List<RecollectionResult> results = new LinkedList<>();

        try {
            serviceScope.select(new TiesServiceScopeRecollection() {

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
                                        convertArguments(retrieve.getArguments(), arguments::add);
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

                                    @Override
                                    public String getType() {
                                        return retrieve.getType();
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
                                convertArguments(filter.getArguments(), arguments::add);
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

                @Override
                public void addResult(Result r) throws TiesServiceScopeException {
                    LOG.debug("AddedResult {}", r);
                    RecollectionResult result = responseController.convertRecollectionResult(request, r);
                    LOG.debug("ConvertedResult {}", result);
                    results.add(result);
                }

            });

            return new RecollectionResponse() {

                @Override
                public BigInteger getMessageId() {
                    return messageId;
                }

                @Override
                public Iterable<RecollectionResult> getResults() {
                    return results;
                }

            };

        } catch (TiesServiceScopeException e) {
            LOG.error("Error handling RecollectionRequest {}", request, e);
            throw new TiesDBProtocolMessageException(messageId, "Error handling RecollectionRequest", e);
        }

    }

}
