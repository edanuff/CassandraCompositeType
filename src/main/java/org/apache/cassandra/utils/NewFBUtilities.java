package org.apache.cassandra.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;

public class NewFBUtilities extends FBUtilities {

	private static final Map<String, AbstractType> comparatorCache = new HashMap<String, AbstractType>();

	public static AbstractType getComparator(String compareWith)
			throws ConfigurationException {
		// The speed up of this with a cache make sense because of the
		// DynamicCompositeType
		AbstractType type = comparatorCache.get(compareWith);

		if (type != null) {
			return type;
		}

		String className = compareWith.contains(".") ? compareWith
				: "org.apache.cassandra.db.marshal." + compareWith;
		Class<? extends AbstractType> typeClass = FBUtilities
				.<AbstractType> classForName(className, "abstract-type");
		try {
			Field field = typeClass.getDeclaredField("instance");
			type = (AbstractType) field.get(null);
			// no synchronization, it's ok for two thread to do this since
			// AbstractType's are singletons
			comparatorCache.put(compareWith, type);
			return type;
		} catch (NoSuchFieldException e) {
			ConfigurationException ex = new ConfigurationException(
					"Invalid comparator " + compareWith
							+ " : must define a public static instance field.");
			ex.initCause(e);
			throw ex;
		} catch (IllegalAccessException e) {
			ConfigurationException ex = new ConfigurationException(
					"Invalid comparator " + compareWith
							+ " : must define a public static instance field.");
			ex.initCause(e);
			throw ex;
		}
	}

}
