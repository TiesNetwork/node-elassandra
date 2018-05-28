/*
 * Copyright 2017 Ties BV
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package network.tiesdb.handler.impl.v0r0;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.TiesDBProtocolManager;
import com.tiesdb.protocol.api.TiesDBProtocol;
import com.tiesdb.protocol.api.TiesDBProtocolHandler;
import com.tiesdb.protocol.api.TiesDBProtocolHandlerProvider;
import com.tiesdb.protocol.api.Version;
import com.tiesdb.protocol.exception.TiesDBException;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesHandlerConfig;
import network.tiesdb.exception.TiesException;
import network.tiesdb.handler.api.TiesHandler;
import network.tiesdb.handler.impl.v0r0.controller.Request;
import network.tiesdb.handler.impl.v0r0.controller.RequestController;
import network.tiesdb.handler.impl.v0r0.processor.RequestProcessor;
import network.tiesdb.handler.impl.v0r0.util.StreamInput;
import network.tiesdb.handler.impl.v0r0.util.StreamOutput;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.transport.api.TiesRequest;
import network.tiesdb.transport.api.TiesResponse;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

/**
 * TiesDB handler implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesHandlerImpl implements TiesHandler, TiesDBProtocolHandler<TiesDBProtocolV0R0.Conversation>, TiesDBProtocolHandlerProvider {

    private static final TiesHandlerImplVersion IMPLEMENTATION_VERSION = TiesHandlerImplVersion.v_0_0_1_prealpha;

    private static final Logger LOG = LoggerFactory.getLogger(TiesHandlerImpl.class);

    // private final TiesService service;

    private final TiesHandlerConfigImpl config;

    private final Collection<TiesDBProtocol> protocols;

    private final RequestController requestController;

    private final RequestProcessor requestProcessor;

    public TiesHandlerImpl(TiesService service, TiesHandlerConfigImpl config) {
        if (null == config) {
            throw new NullPointerException("The config should not be null");
        }
        if (null == service) {
            throw new NullPointerException("The service should not be null");
        }
        // this.service = service;
        this.config = config;

        this.protocols = TiesDBProtocolManager.getProtocols();
        if (protocols.isEmpty()) {
            throw new RuntimeException("No TiesDBProtocols found");
        }

        this.requestController = new RequestController();

        this.requestProcessor = new RequestProcessor(service);
    };

    @Override
    public TiesVersion getVersion() {
        return IMPLEMENTATION_VERSION;
    }

    @Override
    public TiesHandlerConfig getTiesHandlerConfig() {
        return config;
    }

    @Override
    public void handle(final TiesRequest tiesRequest, final TiesResponse tiesResponse) throws TiesException {
        LOG.trace("Call to network.tiesdb.handler.impl.v0r0.TiesHandlerImpl.handle(request, response)");
        for (TiesDBProtocol protocol : protocols) {
            try {
                final StreamOutput po = new StreamOutput(tiesResponse.getOutputStream());
                final StreamInput pi = new StreamInput(tiesRequest.getInputStream());
                LOG.debug("TiesDBProtocol selected {}", protocol);
                protocol.acceptChannel(pi, po, this);
                break;
            } catch (TiesDBException e) {
                LOG.error("Can't handle request {}", tiesRequest, e);
                throw new TiesException("Can't handle request " + tiesRequest, e);
            }
        }

    }

    @Override
    public void handle(Conversation session) throws TiesDBException {
        AtomicReference<Request> requestRef = new AtomicReference<>();
        try {
            Event event;
            while (null != requestController && null != (event = session.get())) {
                LOG.debug("RootBeginEvent: {}", event);
                if (EventState.BEGIN.equals(event.getState())) {
                    // System.out.println("\t BEGINEvent: " + event);
                    if (requestController.accept(session, event, requestRef)) {
                        try {
                            requestProcessor.processRequest(requestRef.get());
                        } catch (TiesServiceScopeException e) {
                            throw new TiesDBException("Request failed", e);
                        }
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
        } catch (Exception e) {
            session.accept(new Event(TiesDBType.ERROR, EventState.BEGIN));
            for (Throwable th = e; null != th; th = th.getCause()) {
                session.accept(new Event(TiesDBType.ERROR_MESSAGE, EventState.BEGIN));
                session.write(UTF8StringFormat.INSTANCE, th.getClass().getSimpleName() + ": " + th.getMessage());
                session.accept(new Event(TiesDBType.ERROR_MESSAGE, EventState.END));
            }
            session.accept(new Event(TiesDBType.ERROR, EventState.END));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> TiesDBProtocolHandler<? extends S> getHandler(Version localVersion, Version remoteVersion, S session)
            throws TiesDBProtocolException {
        if (!TiesDBProtocolV0R0.Conversation.class.isInstance(session)) {
            throw new TiesDBProtocolException("Protocol negotiation was not implemented yet");
        }
        return (TiesDBProtocolHandler<S>) this;
    }

}
