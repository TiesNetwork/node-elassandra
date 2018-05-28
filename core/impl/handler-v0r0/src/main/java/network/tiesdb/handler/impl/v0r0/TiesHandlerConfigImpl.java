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

import network.tiesdb.context.api.TiesHandlerConfig;
import network.tiesdb.context.api.annotation.TiesConfigElement;

/**
 * TiesDB handler configuration implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
@TiesConfigElement({ TiesHandlerConfigImpl.BINDING, TiesHandlerConfigImpl.SHORT_BINDING })
public class TiesHandlerConfigImpl implements TiesHandlerConfig {

	static final String BINDING = "network.tiesdb.service.HandlerV0R0";
	static final String SHORT_BINDING = "HandlerV0R0";

	public TiesHandlerConfigImpl() {
		// NOP Is not empty config values
	}

	public TiesHandlerConfigImpl(String value) {
		// NOP If this constructor is called then config values is empty and we
		// should use default
	}

	@Override
	public TiesHandlerFactoryImpl getTiesHandlerFactory() {
		return new TiesHandlerFactoryImpl(this);
	}

}
