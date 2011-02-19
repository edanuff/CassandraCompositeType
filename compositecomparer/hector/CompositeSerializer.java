/*******************************************************************************
 * Copyright 2010,2011 Ed Anuff and Usergrid, all rights reserved.
 ******************************************************************************/
package compositecomparer.hector;

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
