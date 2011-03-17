package newcompositecomparer;

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
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The encoding of a DynamicCompositeType column name should be:
 *   <component><component><component> ...
 * where <component> is:
 *   <comparator part><value><'end-of-component' byte>
 * where:
 *   - <comparator part>: either the comparator full name, or a declared
 *     aliases. This is at least 2 bytes (those 2 bytes are called header in
 *     the following). If the first bit of the header is 1, then this
 *     comparator part is an alias, otherwise it's a comparator full name:
 *       - aliases: the actual alias is the 2nd byte of header taken as a
 *         character. The whole <comparator part> is thus 2 byte long.
 *       - comparator full name: the header is the length of the remaining
 *         part. The remaining part is the UTF-8 encoded comparator class
 *         name.
 *   - <value>: the component value bytes preceded by 2 byte containing the
 *     size of value.
 *   - 'end-of-component' byte is defined as in CompositeType
 */
public class DynamicCompositeType extends AbstractCompositeType {
	private static final Logger logger = LoggerFactory
			.getLogger(DynamicCompositeType.class);

	private final Map<Byte, AbstractType> aliases;

	public DynamicCompositeType() {
		aliases = new HashMap<Byte, AbstractType>();
		aliases.put((byte) 'a', AsciiType.instance);
		aliases.put((byte) 'b', BytesType.instance);
		aliases.put((byte) 'i', IntegerType.instance);
		aliases.put((byte) 'x', LexicalUUIDType.instance);
		aliases.put((byte) 'l', LongType.instance);
		aliases.put((byte) 't', TimeUUIDType.instance);
		aliases.put((byte) 's', UTF8Type.instance);
	}

	private AbstractType getComparator(ByteBuffer bb) {
		try {
			int header = getShortLength(bb);
			if ((header & 0x8000) == 0) {
				String name = ByteBufferUtil.string(getBytes(bb, header));
				return FBUtilities.getComparator(name);
			} else {
				return aliases.get((byte) (header & 0xFF));
			}
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected AbstractType getNextComparator(int i, ByteBuffer bb) {
		return getComparator(bb);
	}

	@Override
	protected AbstractType getNextComparator(int i, ByteBuffer bb1,
			ByteBuffer bb2) {
		AbstractType comp1 = getComparator(bb1);
		AbstractType comp2 = getComparator(bb2);

		// This rely on comparator always being singleton instances
		if (comp1 != comp2) {
			logger.error("Mismatch between {} and {}", comp1, comp2);
			throw new RuntimeException(
					"Comparator mismatch while comparing two DynamicCompositeType colum name");
		}
		return comp1;
	}

	@Override
	protected AbstractType getAndAppendNextComparator(int i, ByteBuffer bb,
			StringBuilder sb) {
		try {
			int header = getShortLength(bb);
			if ((header & 0x8000) == 0) {
				String name = ByteBufferUtil.string(getBytes(bb, header));
				sb.append(name).append("@");
				return FBUtilities.getComparator(name);
			} else {
				sb.append((char) (header & 0xFF)).append("@");
				return aliases.get((byte) (header & 0xFF));
			}
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected ParsedComparator parseNextComparator(int i, String part) {
		return new DynamicParsedComparator(part);
	}

	@Override
	protected AbstractType validateNextComparator(int i, ByteBuffer bb)
			throws MarshalException {
		AbstractType comparator = null;
		if (bb.remaining() < 2) {
			throw new MarshalException(
					"Not enough bytes to header of the comparator part of component "
							+ i);
		}
		int header = getShortLength(bb);
		if ((header & 0x8000) == 0) {
			if (bb.remaining() < header) {
				throw new MarshalException(
						"Not enough bytes to read comparator name of component "
								+ i);
			}

			ByteBuffer value = getBytes(bb, header);
			try {
				comparator = FBUtilities.getComparator(ByteBufferUtil
						.string(value));
			} catch (Exception e) {
				// we'll deal with this below since comparator == null
			}
		} else {
			comparator = aliases.get((byte) (header & 0xFF));
		}

		if (comparator == null) {
			throw new MarshalException("Cannot find comparator for component "
					+ i);
		} else {
			return comparator;
		}
	}

	private class DynamicParsedComparator implements ParsedComparator {
		final AbstractType type;
		final boolean isAlias;
		final String comparatorName;
		final String remainingPart;

		DynamicParsedComparator(String part) {
			String[] splits = part.split("@");
			if (splits.length != 2) {
				throw new IllegalArgumentException(
						"Invalid component representation: " + part);
			}

			comparatorName = splits[0];
			remainingPart = splits[1];

			try {
				AbstractType t = null;
				if (comparatorName.length() == 1) {
					// try for an alias
					// Note: the char to byte cast is theorically bogus for
					// unicode character. I take full
					// responsibility if someone get hit by this (without making
					// it on purpose)
					t = aliases.get((byte) comparatorName.charAt(0));
				}
				isAlias = t != null;
				if (!isAlias) {
					t = FBUtilities.getComparator(comparatorName);
				}
				type = t;
			} catch (ConfigurationException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public AbstractType getAbstractType() {
			return type;
		}

		@Override
		public String getRemainingPart() {
			return remainingPart;
		}

		@Override
		public int getComparatorSerializedSize() {
			return isAlias ? 2 : 2 + ByteBufferUtil.bytes(comparatorName)
					.remaining();
		}

		@Override
		public void serializeComparator(ByteBuffer bb) {
			int header = 0;
			if (isAlias) {
				header = 0x8000 | ((byte) comparatorName.charAt(0));
			} else {
				header = comparatorName.length();
			}
			putShortLength(bb, header);

			if (!isAlias) {
				bb.put(ByteBufferUtil.bytes(comparatorName));
			}
		}
	}
}
