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
package network.tiesdb.handler.impl.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesHandlerConfig;
import network.tiesdb.exception.TiesException;
import network.tiesdb.handler.api.TiesHandler;
import network.tiesdb.handler.impl.json.data.TiesJsonRequestError;
import network.tiesdb.handler.impl.json.data.request.TiesJsonRequestRoot;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.transport.api.TiesRequest;
import network.tiesdb.transport.api.TiesResponse;
import network.tiesdb.transport.api.TiesTransport;

/**
 * TiesDB handler implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesHandlerImpl implements TiesHandler {

	private static final TiesHandlerImplVersion IMPLEMENTATION_VERSION = TiesHandlerImplVersion.v_0_0_1_prealpha;

	private static final Logger logger = LoggerFactory.getLogger(TiesHandlerImpl.class);

	private final TiesService service;

	private final TiesHandlerConfigImpl config;

	public TiesHandlerImpl(TiesService service, TiesHandlerConfigImpl config) {
		if (null == config) {
			throw new NullPointerException("The config should not be null");
		}
		if (null == service) {
			throw new NullPointerException("The service should not be null");
		}
		this.service = service;
		this.config = config;
	};

	@Override
	public void handle(final TiesRequest request, final TiesResponse response) throws TiesException {
		logger.trace("Call to network.tiesdb.handler.impl.json.TiesHandlerImpl.handle(request, response)");
		try {
			handleInternal1(request, response);
		} catch (IOException e) {
			throw new TiesException("Can't process request", e);
		}
	}

	protected void handleInternal1(TiesRequest request, TiesResponse response)
			throws IOException {
		ObjectMapper mapper = createConfiguredMapper();
		try {
			TiesJsonRequestRoot jsonRequest = mapper.readValue(request.getInputStream(), TiesJsonRequestRoot.class);
			try (OutputStream os = response.getOutputStream()) {
				mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), jsonRequest);
			}
		} catch (JsonMappingException e) {
			logger.warn("Can't process", e);
			try (OutputStream os = response.getOutputStream()) {
				mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), TiesJsonRequestError.create(e));
			}
		}
	}

	private ObjectMapper createConfiguredMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		mapper.configure(Feature.ALLOW_COMMENTS, true);
		mapper.setSerializationInclusion(Inclusion.NON_NULL);
		return mapper;
	}

	protected void handleInternal(TiesRequest request, TiesResponse response) {
		ObjectMapper mapper = createConfiguredMapper();
		try (InputStream is = request.getInputStream()) {
			@SuppressWarnings("unchecked")
			Map<String, Object> jsonMap = mapper.readValue(is, Map.class);
			Iterator<Entry<String, Object>> iter = jsonMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, Object> entry = iter.next();
				iter.remove();
				jsonMap.put(entry.getKey().toLowerCase(), entry.getValue().toString().toUpperCase());
			}

			jsonMap.put("serviceVersion", service.getVersion());

			List<Map<?, ?>> transportsVersions = new ArrayList<>();
			for (TiesTransport t : service.getTransports()) {
				HashMap<Object, Object> map = new HashMap<>();
				map.put("transportVersion", t.getVersion());
				map.put("handlerVersion", t.getHandler().getVersion());
				transportsVersions.add(map);
			}
			jsonMap.put("transportsVersions", transportsVersions);
			try (OutputStream os = response.getOutputStream()) {
				os.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMap)
						.getBytes(config.getCharset()));
			}
			System.out.println(jsonMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public TiesVersion getVersion() {
		return IMPLEMENTATION_VERSION;
	}

	@Override
	public TiesHandlerConfig getTiesHandlerConfig() {
		return config;
	}

}
