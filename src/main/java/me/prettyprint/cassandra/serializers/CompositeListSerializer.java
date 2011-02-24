package me.prettyprint.cassandra.serializers;

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
import java.util.List;

import org.apache.cassandra.db.marshal.Composite;

public class CompositeListSerializer extends AbstractSerializer<List<Object>> {

	public CompositeListSerializer() {
	}

	@Override
	public byte[] toBytes(List<Object> objects) {
		return Composite.serialize(objects);
	}

	@Override
	public List<Object> fromBytes(byte[] bytes) {
		return Composite.deserialize(bytes);
	}

	@Override
	public ByteBuffer toByteBuffer(List<Object> objects) {
		return ByteBuffer.wrap(Composite.serialize(objects));
	}

	@Override
	public List<Object> fromByteBuffer(ByteBuffer byteBuffer) {
		return Composite.deserialize(byteBuffer);
	}

}
