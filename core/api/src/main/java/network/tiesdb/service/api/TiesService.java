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
package network.tiesdb.service.api;

import java.util.List;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.transport.api.TiesTransport;

/**
 * TiesDB service API.
 * 
 * <P>Defines common service functions.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public interface TiesService {

	TiesServiceDaemon getDaemon();
	
	List<TiesTransport> getTransports();

	TiesServiceConfig getTiesServiceConfig();

	TiesVersion getVersion();
	
	TiesServiceScope newServiceScope();

}
