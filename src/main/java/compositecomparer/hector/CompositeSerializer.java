package compositecomparer.hector;

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

/**
 * CompositeSerializer for encoding Composite values.
 * <p>
 * See the {@link org.apache.cassandra.db.marshal.Composite Composite}
 * class for more details on how to construct and use composites.
 * 
 * @author Ed Anuff
 * @see <a
 *      href="http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html">Secondary
 *      indexes in Cassandra</a>
 * @see "org.apache.cassandra.db.marshal.Composite"
 * 
 */

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

import compositecomparer.Composite;

public class CompositeSerializer extends AbstractSerializer<Composite> {

	public CompositeSerializer() {
	}

	@Override
	public byte[] toBytes(Composite obj) {
		return obj.serialize();
	}

	@Override
	public Composite fromBytes(byte[] bytes) {
		return new Composite(bytes);
	}

	@Override
	public ByteBuffer toByteBuffer(Composite obj) {
		return ByteBuffer.wrap(obj.serialize());
	}

	@Override
	public Composite fromByteBuffer(ByteBuffer byteBuffer) {
		return new Composite(byteBuffer);
	}

}
