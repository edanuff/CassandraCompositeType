package compositecomparer;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * CompositeType comparer for Cassandra columns.
 * <p>
 * CompositeType is a comparer that allows you to combine the existing Cassandra
 * types into a composite type that will then be compared correctly for each of
 * the component types.
 * <p>
 * To use this, you must specify the comparer in your cassandra.yaml file or
 * when you create your column programattically:
 * <p>
 * <code>
 * column_families:
 *   - name: Standard1
 *     compare_with: CompositeType
 * </code>
 * <p>
 * See the {@link org.apache.cassandra.db.marshal.Composite Composite} class for
 * more details on how to construct composites.
 * 
 * @author Ed Anuff
 * @see <a
 *      href="http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html">Secondary
 *      indexes in Cassandra</a>
 * @see "org.apache.cassandra.db.marshal.Composite"
 */

public class CompositeType extends AbstractType {

	/**
	 * 
	 */
	public static final CompositeType instance = new CompositeType();

	/**
	 * 
	 */
	public CompositeType() {
	}

	static final Logger logger = Logger
			.getLogger(CompositeType.class.getName());

	@Override
	public void validate(ByteBuffer buffer) {
		Composite.validate(buffer, true);
	}

	@Override
	public String getString(ByteBuffer buffer) {
		Composite c = new Composite(buffer);
		return c.toString();
	}

	@Override
	public int compare(ByteBuffer b1, ByteBuffer b2) {
		return Composite.compare(b1, b2);
	}

}
