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
package network.tiesdb.handler.impl.json.data;

import java.util.List;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.JsonMappingException.Reference;
import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;

public class TiesJsonRequestError {
	public final boolean error = true;
	public String message;

	private TiesJsonRequestError(String message) {
		this.message = message;
	}

	private static String formatPath(List<Reference> path) {
		StringBuilder sb = new StringBuilder();
		for (Reference reference : path) {
			sb.append(reference.getFieldName());
			sb.append('.');
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	public static Object create(JsonMappingException e) {
		if (e instanceof UnrecognizedPropertyException) {
			return new TiesJsonRequestError("Unrecognized field " + formatPath(e.getPath()));
		}
		return new TiesJsonRequestError("Unexpected exception");
	}
}
