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
package network.tiesdb.handler.impl.json.data.request;

/**
 * Root of TiesDB JSON request.
 * 
 * <P>Any class implementing this interface can be a JSON request root.
 *  
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesJsonRequestRoot {

	public enum RequestType {
		INSERT, SELECT
	}

	private Object request;
	private RequestType type;

	private void setRequest(Object request, RequestType type) {
		this.request = request;
		this.type = type;
	}

	public void setInsert(TiesJsonRequestInsert request) {
		setRequest(request, RequestType.INSERT);
	}

	public void setSelect(TiesJsonRequestSelect request) {
		setRequest(request, RequestType.SELECT);
	}

	public Object getRequest() {
		return request;
	}

	public RequestType getType() {
		return type;
	}

	public String getInnerType() {
		return request.getClass().getName();
	}

}
