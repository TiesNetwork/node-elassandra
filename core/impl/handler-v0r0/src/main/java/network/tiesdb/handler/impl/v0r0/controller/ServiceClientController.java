package network.tiesdb.handler.impl.v0r0.controller;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.ebml.TiesDBRequestConsistency;
import com.tiesdb.protocol.v0r0.ebml.TiesDBRequestConsistency.ConsistencyType;
import com.tiesdb.protocol.v0r0.writer.EntryHeaderWriter.EntryHeader;
import com.tiesdb.protocol.v0r0.writer.FieldWriter.Field;
import com.tiesdb.protocol.v0r0.writer.ModificationEntryWriter.ModificationEntry;
import com.tiesdb.protocol.v0r0.writer.ModificationRequestWriter;
import com.tiesdb.protocol.v0r0.writer.ModificationRequestWriter.ModificationRequest;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesEntryHeader;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeAction.Distributed.ActionConsistency;
import network.tiesdb.service.scope.api.TiesServiceScopeAction.Distributed.ActionConsistency.CountConsistency;
import network.tiesdb.service.scope.api.TiesServiceScopeAction.Distributed.ActionConsistency.PercentConsistency;
import network.tiesdb.service.scope.api.TiesServiceScopeAction.Distributed.ActionConsistency.QuorumConsistency;
import network.tiesdb.service.scope.api.TiesServiceScopeResult;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeModification;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry.FieldHash;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry.FieldValue;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection;
import network.tiesdb.service.scope.api.TiesServiceScopeSchema;
import one.utopic.sparse.ebml.EBMLFormat;
import one.utopic.sparse.ebml.format.BytesFormat;

public class ServiceClientController implements TiesServiceScope {

    private static final ActionConsistency.Visitor<TiesDBRequestConsistency> CONSISTENCY_SELECTOR = new ActionConsistency.Visitor<TiesDBRequestConsistency>() {

        @Override
        public TiesDBRequestConsistency on(CountConsistency countConsistency) {
            return new TiesDBRequestConsistency(ConsistencyType.COUNT, countConsistency.getValue());
        }

        @Override
        public TiesDBRequestConsistency on(PercentConsistency percentConsistency) {
            return new TiesDBRequestConsistency(ConsistencyType.PERCENT, percentConsistency.getValue());
        }

        @Override
        public TiesDBRequestConsistency on(QuorumConsistency quorumConsistency) {
            return new TiesDBRequestConsistency(ConsistencyType.QUORUM, 0);
        }

    };

    private final Conversation session;
    private final TiesService service;

    public ServiceClientController(TiesService service, Conversation session) {
        this.service = service;
        this.session = session;
    }

    @Override
    public void close() throws IOException {
        // NOP
    }

    @Override
    public void insert(TiesServiceScopeModification action) throws TiesServiceScopeException {
        try {
            Entry entry = action.getEntry();
            if (null == entry) {
                throw new TiesServiceScopeException("No entry found in modification request");
            }
            TiesEntryHeader entryHeader = entry.getHeader();
            if (null == entryHeader) {
                throw new TiesServiceScopeException("No header found in modification request entry");
            }
            new ModificationRequestWriter().accept(session, new ModificationRequest() {

                @Override
                public TiesDBRequestConsistency getConsistency() {
                    return action.getConsistency().accept(CONSISTENCY_SELECTOR);
                }

                @Override
                public Iterable<ModificationEntry> getEntries() {
                    return Arrays.asList(new ModificationEntry() {

                        private final Iterable<Field> fields;
                        {
                            Map<String, FieldHash> fieldHashes = entry.getFieldHashes();
                            Map<String, FieldValue> fieldValues = entry.getFieldValues();
                            HashSet<String> fieldNames = new HashSet<>(fieldHashes.size() + fieldValues.size());
                            fieldNames.addAll(fieldHashes.keySet());
                            fieldNames.addAll(fieldValues.keySet());
                            LinkedList<Field> fieldsCache = new LinkedList<>();
                            for (final String name : fieldNames) {
                                {
                                    FieldValue field = fieldValues.get(name);
                                    if (null != field) {
                                        fieldsCache.add(new Field.ValueField<byte[]>() {

                                            @Override
                                            public String getName() {
                                                return name;
                                            }

                                            @Override
                                            public String getType() {
                                                return field.getType();
                                            }

                                            @Override
                                            public EBMLFormat<byte[]> getFormat() {
                                                return BytesFormat.INSTANCE;
                                            }

                                            @Override
                                            public byte[] getValue() {
                                                return field.getBytes();
                                            }
                                        });
                                        continue;
                                    }
                                }
                                {
                                    FieldHash field = fieldHashes.get(name);
                                    if (null != field) {
                                        fieldsCache.add(new Field.HashField() {

                                            @Override
                                            public String getName() {
                                                return name;
                                            }

                                            @Override
                                            public String getType() {
                                                return field.getType();
                                            }

                                            @Override
                                            public byte[] getHash() {
                                                return field.getHash();
                                            }
                                        });
                                        continue;
                                    }
                                }
                            }
                            fields = Collections.unmodifiableList(fieldsCache);
                        }

                        @Override
                        public Iterable<Field> getFields() {
                            return fields;
                        }

                        @Override
                        public EntryHeader getHeader() {
                            return new EntryHeader() {

                                TiesEntryHeader header = entry.getHeader();

                                @Override
                                public byte[] getSigner() {
                                    return header.getSigner();
                                }

                                @Override
                                public byte[] getSignature() {
                                    return header.getSignature();
                                }

                                @Override
                                public String getTablespaceName() {
                                    return entry.getTablespaceName();
                                }

                                @Override
                                public String getTableName() {
                                    return entry.getTableName();
                                }

                                @Override
                                public BigInteger getEntryVersion() {
                                    return header.getEntryVersion();
                                }

                                @Override
                                public Date getEntryTimestamp() {
                                    return header.getEntryTimestamp();
                                }

                                @Override
                                public byte[] getEntryOldHash() {
                                    return header.getEntryOldHash();
                                }

                                @Override
                                public Integer getEntryNetwork() {
                                    return Short.toUnsignedInt(header.getEntryNetwork());
                                }

                                @Override
                                public byte[] getEntryFldHash() {
                                    return header.getEntryFldHash();
                                }
                            };
                        }

                    });
                }

                @Override
                public BigInteger getMessageId() {
                    return action.getMessageId();
                }

            });
            action.addResult(new TiesServiceScopeModification.Result.Success() {
                @Override
                public byte[] getHeaderHash() {
                    return entryHeader.getHash();
                }
            });
        } catch (TiesDBProtocolException e) {
            throw new TiesServiceScopeException("Node modification request failed", e);
        }
    }

    @Override
    public void update(TiesServiceScopeModification action) throws TiesServiceScopeException {
        // TODO Auto-generated method stub
        throw new TiesServiceScopeException("Not implemented");
    }

    @Override
    public void delete(TiesServiceScopeModification action) throws TiesServiceScopeException {
        // TODO Auto-generated method stub
        throw new TiesServiceScopeException("Not implemented");
    }

    @Override
    public void select(TiesServiceScopeRecollection query) throws TiesServiceScopeException {
        // TODO Auto-generated method stub
        throw new TiesServiceScopeException("Not implemented");
    }

    @Override
    public void schema(TiesServiceScopeSchema query) throws TiesServiceScopeException {
        // TODO Auto-generated method stub
        throw new TiesServiceScopeException("Not implemented");
    }

    @Override
    public TiesVersion getServiceVersion() {
        return service.getVersion();
    }

    @Override
    public void result(TiesServiceScopeResult result) throws TiesServiceScopeException {
        throw new TiesServiceScopeException("Client should not handle any result");
    }

}
