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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.db.marshal.MarshalException;

/**
 * Composite type for Cassandra columns.
 * <p>
 * Composite allows you to combine the existing Cassandra types into a composite
 * type that will then be compared correctly for each of the component types.
 * <p>
 * To construct a composite name for a new column, use the following:
 * <p>
 * <code>
 * Composite c = new Composite();<br>
 * c.addUTF8("smith").addUTF8("bob").addLexicalUUID(new UUID());<br>
 * byte[] bytes = c.serialize();<br>
 * </code>
 * <p>
 * The actual composite type consists of four byte prefix that includes an
 * identifier and version number, followed by, for each embedded component, 1
 * byte to specify the component type, and then for variable length types such
 * as ASCII strings, 2 bytes are used for the length of the following data, and
 * then the actual component data.
 * 
 * @author Ed Anuff
 * @see <a
 *      href="http://www.anuff.com/2010/07/secondary-indexes-in-cassandra.html">Secondary
 *      indexes in Cassandra</a>
 * @see "org.apache.cassandra.db.marshal.CompositeType"
 */

public class Composite implements Collection<Object>, Comparable<Composite> {

	/**
	 * Byte id code to help detect byte stream contains serialized Composite
	 * value.
	 */
	public final static int COMPOSITETYPE_ID_0 = 'C';
	public final static int COMPOSITETYPE_ID_1 = 'M';
	public final static int COMPOSITETYPE_ID_2 = 'P';

	public final static int FIRST_BYTE_OFFSET = 4;

	public final static int MIN_BYTE_COUNT = 5;

	/**
	 * Byte version number of Composite format.
	 */
	public final static int COMPOSITETYPE_VERSION = 1;

	/**
	 * Component id for placeholder matching the minimum possible value
	 */
	public final static int COMPONENT_STOP = 0;

	/**
	 * Component id for placeholder matching the minimum possible value
	 */
	public final static int COMPONENT_MINIMUM = 1;

	/**
	 * Component id for boolean values
	 */
	public final static int COMPONENT_BOOL = 2;

	/**
	 * Component id for long values
	 */
	public final static int COMPONENT_LONG = 3;

	/**
	 * Component id for real values
	 */
	public final static int COMPONENT_REAL = 4;

	/**
	 * Component id for timeuuid values
	 */
	public final static int COMPONENT_TIMEUUID = 5;

	/**
	 * Component id for lexical uuid values
	 */
	public final static int COMPONENT_LEXICALUUID = 6;

	/**
	 * Component id for ascii character strings
	 */
	public final static int COMPONENT_ASCII = 7;

	/**
	 * Component id for utf8 encoded strings
	 */
	public final static int COMPONENT_UTF8 = 8;

	/**
	 * Component id for byte array values
	 */
	public final static int COMPONENT_BYTES = 9;

	/**
	 * Component id for place holder matching the maximum possible value.
	 */
	public final static int COMPONENT_MAXIMUM = 255;

	/**
	 * UTF8 string encoding
	 */
	public static final String UTF8_ENCODING = "UTF-8";

	/**
	 * ASCII string encoding
	 */
	public static final String ASCII_ENCODING = "US-ASCII";

	static final Logger logger = Logger.getLogger(Composite.class.getName());

	/**
	 * Convenience instance of minimum matching place holder
	 */
	public static Placeholder MATCH_MINIMUM = new Placeholder(COMPONENT_MINIMUM);

	/**
	 * Convenience instance of maximum matching place holder
	 */
	public static Placeholder MATCH_MAXIMUM = new Placeholder(COMPONENT_MAXIMUM);

	/**
	 * The Class Placeholder.
	 * 
	 * @author edanuff
	 */
	public static class Placeholder {
		int type;

		private Placeholder(int type) {
			this.type = type;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof Placeholder) {
				return type == ((Placeholder) o).type;
			}
			return false;
		}

		@Override
		public String toString() {
			return "Placeholder(" + type + ")";
		}
	}

	int startOffset;
	byte[] bytes;
	ByteArrayOutputStream byteStream;
	DataOutputStream out;

	/**
	 * 
	 */
	public Composite() {
	}

	/**
	 * @param bytes
	 */
	public Composite(byte[] bytes) {
		this.bytes = bytes;
	}

	public Composite(ByteBuffer buffer) {
		bytes = buffer.array();
		startOffset = buffer.arrayOffset() + buffer.position();
	}

	/**
	 * @param objects
	 */
	public Composite(Object... objects) {
		for (Object obj : objects) {
			add(obj);
		}
	}

	/**
	 * @param objects
	 */
	public Composite(List<Object> objects) {
		for (Object obj : objects) {
			add(obj);
		}
	}

	private void initOutputStream() {
		if (byteStream == null) {
			byteStream = new ByteArrayOutputStream();
			out = new DataOutputStream(byteStream);
			if (bytes == null) {
				try {
					out.write(COMPOSITETYPE_ID_0);
					out.write(COMPOSITETYPE_ID_1);
					out.write(COMPOSITETYPE_ID_2);
					out.write(COMPOSITETYPE_VERSION);
				} catch (IOException e) {
					logger.throwing("Composite", "initOutputStream", e);
				}
			} else {
				try {
					out.write(bytes);
				} catch (IOException e) {
					logger.throwing("Composite", "initOutputStream", e);
				}
			}
		}
	}

	private void pack() {
		if (byteStream != null) {
			try {
				out.write(COMPONENT_STOP);
			} catch (IOException e) {
				logger.throwing("Composite", "pack", e);
			}
			bytes = byteStream.toByteArray();
			byteStream = null;
			out = null;
		}
	}

	@Override
	public String toString() {
		Iterator<Object> iter = iterator();
		if (!iter.hasNext()) {
			return "";
		}
		StringBuilder builder = new StringBuilder(stringValueOf(iter.next()));
		while (iter.hasNext()) {
			builder.append(',').append(stringValueOf(iter.next()));
		}
		return builder.toString();
	}

	private static String stringValueOf(Object o) {
		if (o instanceof byte[]) {
			return getHexString((byte[]) o, 0, ((byte[]) o).length);
		}
		return String.valueOf(o);
	}

	/**
	 * @param obj
	 * @return true if object is a valid Entity property or property type
	 */
	public static boolean isValidType(Object obj) {
		return (obj instanceof UUID) || (obj instanceof String)
				|| (obj instanceof Long) || (obj instanceof Integer)
				|| (obj instanceof Double) || (obj instanceof Float)
				|| (obj instanceof Boolean) || (obj instanceof byte[]);
	}

	@Override
	public boolean add(Object o) {
		if (o == null) {
			return false;
		}
		if (o instanceof Long) {
			addLong(((Long) o).longValue());
		} else if (o instanceof Integer) {
			addLong(((Integer) o).intValue());
		} else if (o instanceof Double) {
			addReal(((Double) o).doubleValue());
		} else if (o instanceof Float) {
			addReal(((Float) o).doubleValue());
		} else if (o instanceof Boolean) {
			addBool(((Boolean) o).booleanValue());
		} else if (o instanceof String) {
			addUTF8((String) o);
		} else if (o instanceof UUID) {
			if (isTimeBased((UUID) o)) {
				addTimeUUID((UUID) o);
			} else {
				addLexicalUUID((UUID) o);
			}
		} else if (o instanceof byte[]) {
			addBytes((byte[]) o);
		} else if (o instanceof Placeholder) {
			if (MATCH_MAXIMUM.equals(o)) {
				addMatchMaximum();
			} else if (MATCH_MINIMUM.equals(o)) {
				addMatchMinimum();
			}
		} else if (o instanceof Collection<?>) {
			addAll((Collection<?>) o);
		} else {
			throw new ClassCastException();
		}

		return true;
	}

	@Override
	public boolean addAll(Collection<? extends Object> c) {
		for (Iterator<? extends Object> iter = c.iterator(); iter.hasNext();) {
			add(iter.next());
		}
		return true;
	}

	/**
	 * Adds the provided byte array to the composite type being built.
	 * 
	 * @param part
	 *            the component part to append as a byte array
	 * @return the composite type builder for chained invocation
	 */
	public Composite addBytes(byte[] part) {
		initOutputStream();
		try {
			out.write(COMPONENT_BYTES);
			out.writeShort(part.length);
			out.write(part);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided ASCII string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as an ASCII string
	 * @return the composite type builder for chained invocation
	 */
	public Composite addAscii(String str) {
		initOutputStream();
		try {
			byte[] bytes = bytes(str, ASCII_ENCODING);
			out.write(COMPONENT_ASCII);
			out.writeShort(bytes.length);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided UTF8 string to the composite type being built.
	 * 
	 * @param str
	 *            the component part to append as a UTF8 string
	 * @return the composite type builder for chained invocation
	 */
	public Composite addUTF8(String str) {
		initOutputStream();
		try {
			byte[] bytes = bytes(str);
			out.write(COMPONENT_UTF8);
			out.writeShort(bytes.length);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided boolean value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a boolean value
	 * @return the composite type builder for chained invocation
	 */
	public Composite addBool(boolean val) {
		initOutputStream();
		try {
			out.write(COMPONENT_BOOL);
			out.writeBoolean(val);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided long value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a long value
	 * @return the composite type builder for chained invocation
	 */
	public Composite addLong(long val) {
		initOutputStream();
		try {
			out.write(COMPONENT_LONG);
			out.writeLong(val);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the provided real value to the composite type being built.
	 * 
	 * @param val
	 *            the component part to append as a real value
	 * @return the composite type builder for chained invocation
	 */
	public Composite addReal(Double val) {
		initOutputStream();
		try {
			out.write(COMPONENT_REAL);
			out.writeDouble(val);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the time-based UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a time-based UUID
	 * @return the composite type builder for chained invocation
	 */
	public Composite addTimeUUID(UUID uuid) {
		initOutputStream();
		try {
			byte[] bytes = bytes(uuid);
			out.write(COMPONENT_TIMEUUID);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * Adds the lexical UUID to the composite type being built.
	 * 
	 * @param uuid
	 *            the component part to append as a regular UUID
	 * @return the composite type builder for chained invocation
	 */
	public Composite addLexicalUUID(UUID uuid) {
		initOutputStream();
		try {
			byte[] bytes = bytes(uuid);
			out.write(COMPONENT_LEXICALUUID);
			out.write(bytes);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * @return composite value, for chaining method calls
	 */
	public Composite addMatchMinimum() {
		initOutputStream();
		try {
			out.write(COMPONENT_MINIMUM);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	/**
	 * @return composite value, for chaining method calls
	 */
	public Composite addMatchMaximum() {
		initOutputStream();
		try {
			out.write(COMPONENT_MAXIMUM);
		} catch (IOException e) {
			// IOException is never thrown by ByteArrayOutputStream
		}
		return this;
	}

	@Override
	public void clear() {
		bytes = null;
		byteStream = null;
		out = null;
	}

	@Override
	public boolean contains(Object o) {
		for (Iterator<? extends Object> iter = iterator(); iter.hasNext();) {
			Object obj = iter.next();
			if (obj.equals(o)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Iterator<? extends Object> iter = c.iterator(); iter.hasNext();) {
			Object obj = iter.next();
			if (!contains(obj)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty() {
		pack();
		if ((bytes != null) && (bytes.length > startOffset + FIRST_BYTE_OFFSET)) {
			return false;
		}
		return true;
	}

	@Override
	public Iterator<Object> iterator() {
		pack();
		return new CompositeTypeIterator(this, startOffset, bytes);
	}

	class CompositeTypeIterator implements Iterator<Object> {

		Composite collection;
		int start = 0;
		byte[] bytes;
		int offset = 0;
		int len = 0;
		int type = 0;

		CompositeTypeIterator(Composite c, int start, byte[] bytes) {
			collection = c;
			this.start = start;
			this.bytes = bytes;
			offset = start;
		}

		@Override
		public boolean hasNext() {
			if (offset == start) {
				if ((bytes == null) || (bytes.length == start)) {
					return false;
				}

				validate(start, bytes, true);

				offset = start + FIRST_BYTE_OFFSET;
			}
			return ((offset < bytes.length) && (bytes[offset] != COMPONENT_STOP));
		}

		@Override
		public Object next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			int[] offsetRef = new int[1];
			offsetRef[0] = offset;
			Object obj = deserializeBytesAt(bytes, offsetRef);
			offset = offsetRef[0];
			return obj;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		pack();
		List<Object> objects = deserialize(bytes);
		return objects.size();
	}

	@Override
	public Object[] toArray() {
		pack();
		List<Object> objects = deserialize(bytes);
		return objects.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		pack();
		List<Object> objects = deserialize(bytes);
		return objects.toArray(a);
	}

	/**
	 * @return byte array serialized form
	 */
	public byte[] serialize() {
		pack();
		return bytes;
	}

	public ByteBuffer serializeToByteBuffer() {
		return ByteBuffer.wrap(serialize());
	}

	private static byte[] bytes(UUID uuid) {
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}

	private static byte[] bytes(String s) {
		return bytes(s, UTF8_ENCODING);
	}

	private static byte[] bytes(String s, String encoding) {
		try {
			return s.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException ", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public int compareTo(Composite o) {
		pack();
		return compare(serialize(), o.serialize());
	}

	/**
	 * @param c1
	 * @param c2
	 * @return comparison value, -1 for less than, 0 for equals, 1 for greater
	 *         than
	 */
	public static int compare(Composite c1, Composite c2) {
		return compare(c1.serialize(), c2.serialize());
	}

	public static int compare(byte[] o1, byte[] o2) {
		return compare(0, o1, 0, o2);
	}

	public static int compare(ByteBuffer b1, ByteBuffer b2) {
		if ((b1 == null) || (b1.remaining() == 0)) {
			return ((b2 == null) || (b2.remaining() == 0)) ? 0 : -1;
		}
		if ((b2 == null) || (b2.remaining() == 0)) {
			return 1;
		}
		return compare(b1.arrayOffset() + b1.position(), b1.array(),
				b2.arrayOffset() + b2.position(), b2.array());
	}

	/**
	 * @param o1
	 * @param o2
	 * @return comparison value, -1 for less than, 0 for equals, 1 for greater
	 */
	public static int compare(int s1, byte[] o1, int s2, byte[] o2) {

		if ((o1 == null) || (o1.length == s1)) {
			return ((o2 == null) || (o2.length == s2)) ? 0 : -1;
		}
		if ((o2 == null) || (o2.length == s2)) {
			return 1;
		}

		validate(s1, o1, true);
		validate(s2, o2, true);

		int comp = 0;

		int offset1 = s1 + FIRST_BYTE_OFFSET;
		int len1 = 0;
		int type1 = 0;

		int offset2 = s2 + FIRST_BYTE_OFFSET;
		int len2 = 0;
		int type2 = 0;

		while (true) {
			type1 = o1[offset1] & 0xff;
			type2 = o2[offset2] & 0xff;

			if (type1 != type2) {
				if ((type1 == COMPONENT_STOP) && (type2 == COMPONENT_MINIMUM)) {
					return 1;
				} else if ((type2 == COMPONENT_STOP)
						&& (type1 == COMPONENT_MINIMUM)) {
					return -1;
				}
				return type1 < type2 ? -1 : 1;
			}

			if (type1 == COMPONENT_STOP) {
				return 0;
			}

			switch (type1) {
			case COMPONENT_BYTES:
			case COMPONENT_ASCII:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareByteArrays(o1, offset1 + 3, len1, o2,
						offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COMPONENT_UTF8:
				len1 = getShort(o1, offset1 + 1);
				len2 = getShort(o2, offset2 + 1);
				comp = compareUTF8(o1, offset1 + 3, len1, o2, offset2 + 3, len2);
				offset1 += len1 + 3;
				offset2 += len2 + 3;
				break;

			case COMPONENT_BOOL:
				comp = compareBool(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 2;
				offset2 += 2;
				break;

			case COMPONENT_LONG:
				comp = compareLong(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 9;
				offset2 += 9;
				break;

			case COMPONENT_REAL:
				comp = compareReal(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 9;
				offset2 += 9;
				break;

			case COMPONENT_LEXICALUUID:
				comp = compareLexicalUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

			case COMPONENT_TIMEUUID:
				comp = compareTimeUUID(o1, offset1 + 1, o2, offset2 + 1);
				offset1 += 17;
				offset2 += 17;
				break;

			case COMPONENT_MINIMUM:
			case COMPONENT_MAXIMUM:
				offset1 += 1;
				offset2 += 1;

			default:
				throw new MarshalException("Unknown embedded type: " + type1);
			}

			if (comp != 0) {
				return comp;
			}

			// these length checks should no longer happen now that
			// composite types are zero-terminated with a COMPONENT_STOP token

			// is o1 done?
			if (o1.length <= offset1) {
				// is o2 done
				if (o2.length <= offset2) {
					// if so, they're equal
					return 0;
				} else {
					// check o2's next token
					type2 = o2[offset2] & 0xff;
					if (type2 == COMPONENT_MINIMUM) {
						// if it's match_minimum, then it's always less,
						// so return o1 greater
						return 1;
					} else {
						// any other token and it means o2 is greater
						return -1;
					}
				}
			}
			// is o2 done?
			if (o2.length <= offset2) {
				// check o1's next token
				type1 = o1[offset1] & 0xff;
				if (type1 == COMPONENT_MINIMUM) {
					return -1;
				} else {
					return 1;
				}
			}

		}
	}

	private static int getShort(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xff) << 8) + (bytes[offset + 1] & 0xff);
	}

	private static int compareBool(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		boolean b1 = bytes1[offset1] != 0;
		boolean b2 = bytes2[offset2] != 0;
		if (!b1 && b2) {
			return -1;
		}
		if (b1 && !b2) {
			return 1;
		}
		return 0;
	}

	private static int compareLong(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		long L1 = ByteBuffer.wrap(bytes1, offset1, 8).getLong();
		long L2 = ByteBuffer.wrap(bytes2, offset2, 8).getLong();
		return Long.valueOf(L1).compareTo(Long.valueOf(L2));
	}

	private static int compareReal(byte[] bytes1, int offset1, byte[] bytes2,
			int offset2) {

		double L1 = ByteBuffer.wrap(bytes1, offset1, 8).getDouble();
		double L2 = ByteBuffer.wrap(bytes2, offset2, 8).getDouble();
		return Double.valueOf(L1).compareTo(Double.valueOf(L2));
	}

	private static String getHexString(byte[] bytes, int offset, int len) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			int bint = bytes[i + offset] & 0xff;
			if (bint <= 0xF) {
				// toHexString does not 0 pad its results.
				sb.append("0");
			}
			sb.append(Integer.toHexString(bint));
		}
		return sb.toString();
	}

	private static int compareByteArrays(byte[] bytes1, int offset1, int len1,
			byte[] bytes2, int offset2, int len2) {
		if (null == bytes1) {
			if (null == bytes2) {
				return 0;
			} else {
				return -1;
			}
		}
		if (null == bytes2) {
			return 1;
		}

		if (len1 < 0) {
			len1 = bytes1.length - offset1;
		}
		if (len2 < 0) {
			len2 = bytes2.length - offset2;
		}

		int minLength = Math.min(len1, len2);
		for (int i = 0; i < minLength; i++) {
			int i1 = offset1 + i;
			int i2 = offset2 + i;
			if (bytes1[i1] == bytes2[i2]) {
				continue;
			}
			// compare non-equal bytes as unsigned
			return (bytes1[i1] & 0xFF) < (bytes2[i2] & 0xFF) ? -1 : 1;
		}
		if (len1 == len2) {
			return 0;
		} else {
			return (len1 < len2) ? -1 : 1;
		}
	}

	private static int compareUTF8(byte[] bytes1, int offset1, int len1,
			byte[] bytes2, int offset2, int len2) {

		String str1 = new String(bytes1, offset1, len1);
		String str2 = new String(bytes2, offset2, len2);

		/*
		 * int c = str1.compareTo(str2); if (c < 0) { System.out.println(str1 +
		 * " < " + str2); } else if (c > 0) { System.out.println(str1 + " > " +
		 * str2); } else { System.out.println(str1 + " == " + str2); } return c;
		 */
		return str1.compareTo(str2);

	}

	private static UUID getUUID(byte[] bytes, int offset) {

		ByteBuffer bb = ByteBuffer.wrap(bytes, offset, 16);
		return new UUID(bb.getLong(), bb.getLong());

	}

	private static int compareLexicalUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		UUID uuid1 = getUUID(bytes1, offset1);
		UUID uuid2 = getUUID(bytes2, offset2);

		return uuid1.compareTo(uuid2);

	}

	private static int compareTimestampBytes(byte[] o1, int i1, byte[] o2,
			int i2) {
		int d = (o1[i1 + 6] & 0xF) - (o2[i2 + 6] & 0xF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1 + 7] & 0xFF) - (o2[i2 + 7] & 0xFF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1 + 4] & 0xFF) - (o2[i2 + 4] & 0xFF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1 + 5] & 0xFF) - (o2[i2 + 5] & 0xFF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1] & 0xFF) - (o2[i2 + 0] & 0xFF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1 + 1] & 0xFF) - (o2[i2 + 1] & 0xFF);
		if (d != 0) {
			return d;
		}
		d = (o1[i1 + 2] & 0xFF) - (o2[i2 + 2] & 0xFF);
		if (d != 0) {
			return d;
		}
		return (o1[i1 + 3] & 0xFF) - (o2[i2 + 3] & 0xFF);
	}

	private static int compareTimeUUID(byte[] bytes1, int offset1,
			byte[] bytes2, int offset2) {

		int res = compareTimestampBytes(bytes1, offset1, bytes2, offset2);
		if (res != 0) {
			return res;
		}

		return compareByteArrays(bytes1, offset1, 16, bytes2, offset2, 16);

	}

	private static Object deserializeBytesAt(byte[] bytes, int[] offsetRef) {
		Object result = null;
		int len = 0;
		int offset = offsetRef[0];
		int type = bytes[offset] & 0xff;
		// System.out.println(type);

		switch (type) {
		case COMPONENT_STOP:
			return null;

		case COMPONENT_BYTES:
			len = getShort(bytes, offset + 1);
			byte[] b = new byte[len];
			System.arraycopy(bytes, offset + 3, b, 0, len);
			result = b;
			offset += len + 3;
			break;

		case COMPONENT_ASCII:
			len = getShort(bytes, offset + 1);
			try {
				result = new String(bytes, offset + 3, len, ASCII_ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			offset += len + 3;
			break;

		case COMPONENT_UTF8:
			len = getShort(bytes, offset + 1);
			try {
				result = new String(bytes, offset + 3, len, UTF8_ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			offset += len + 3;
			break;

		case COMPONENT_LONG:
			result = ByteBuffer.wrap(bytes, offset + 1, 8).getLong();
			offset += 9;
			break;

		case COMPONENT_LEXICALUUID:
		case COMPONENT_TIMEUUID:
			result = getUUID(bytes, offset + 1);
			offset += 17;
			break;

		case COMPONENT_BOOL:
			result = Boolean.valueOf(bytes[offset + 1] != 0);
			offset += 2;
			break;

		case COMPONENT_MINIMUM:
			result = MATCH_MINIMUM;
			offset += 1;
			break;

		case COMPONENT_MAXIMUM:
			result = MATCH_MAXIMUM;
			offset += 1;
			break;

		default:
			throw new MarshalException("Unknown embedded type at offset: "
					+ offset);
		}

		offsetRef[0] = offset;
		return result;

	}

	/**
	 * @param bytes
	 * @return set of deserialized component objects
	 */
	public static List<Object> deserialize(byte[] bytes) {
		return deserialize(0, bytes);
	}

	public static List<Object> deserialize(ByteBuffer bytes) {
		if (bytes.remaining() == 0) {
			return new ArrayList<Object>();
		}
		return deserialize(bytes.arrayOffset() + bytes.position(),
				bytes.array());
	}

	public static List<Object> deserialize(int s, byte[] bytes) {
		// System.out.println("column name bytes: "
		// + getBytesString(bytes, 0, bytes.length));
		List<Object> results = new ArrayList<Object>();

		if ((bytes == null) || (bytes.length == s)) {
			return results;
		}

		validate(s, bytes, true);

		int offset = s + FIRST_BYTE_OFFSET;
		int[] offsetRef = new int[1];

		while (true) {
			offsetRef[0] = offset;
			Object obj = deserializeBytesAt(bytes, offsetRef);
			if (obj == null) {
				break;
			}

			results.add(obj);
			offset = offsetRef[0];

			if (bytes.length == offset) {
				break;
			}

			if (bytes.length < offset) {
				throw new MarshalException(
						"Incorrect number of bytes, either value is corrupt or not composite type");
			}

		}

		return results;
	}

	/**
	 * @param bytes
	 * @return true if serialized component is value
	 */
	public static boolean validate(int start, byte[] bytes) {
		return validate(start, bytes, false);
	}

	public static boolean validate(ByteBuffer buffer) {
		if (buffer.remaining() == 0) {
			return true;
		}
		return validate(buffer.arrayOffset() + buffer.position(),
				buffer.array(), false);
	}

	public static boolean validate(ByteBuffer buffer, boolean throwException) {
		if (buffer.remaining() == 0) {
			return true;
		}
		return validate(buffer.arrayOffset() + buffer.position(),
				buffer.array(), throwException);
	}

	public static boolean validate(int start, byte[] bytes,
			boolean throwException) {
		if ((bytes != null) && (bytes.length > (start + 4))) {

			if ((bytes[start] & 0xFF) != COMPOSITETYPE_ID_0) {
				if (throwException) {
					throw new MarshalException(
							"Not a composite type (ID byte 0 incorrect)");
				}
				return false;
			}

			if ((bytes[start + 1] & 0xFF) != COMPOSITETYPE_ID_1) {
				if (throwException) {
					throw new MarshalException(
							"Not a composite type (ID byte 1 incorrect)");
				}
				return false;
			}

			if ((bytes[start + 2] & 0xFF) != COMPOSITETYPE_ID_2) {
				if (throwException) {
					throw new MarshalException(
							"Not a composite type (ID byte 2 incorrect)");
				}
				return false;
			}

			if ((bytes[start + 3] & 0xFF) != COMPOSITETYPE_VERSION) {
				if (throwException) {
					throw new MarshalException(
							"Incorrect composite type version for this deserializer, expected "
									+ COMPOSITETYPE_VERSION + ", found "
									+ (bytes[start + 3] & 0xFF));
				}

				return false;
			}
		}
		return true;
	}

	/**
	 * @param objects
	 * @return composite value as serialized byte array
	 */
	public static byte[] serialize(Object... objects) {
		Composite c = new Composite(objects);
		return c.serialize();
	}

	/**
	 * @param objects
	 * @return composite value as serialized byte array
	 */
	public static byte[] serialize(List<Object> objects) {
		Composite c = new Composite(objects);
		return c.serialize();
	}

	public static ByteBuffer serializeToByteBuffer(Object... objects) {
		return ByteBuffer.wrap(serialize(objects));
	}

	public static ByteBuffer serializeToByteBuffer(List<Object> objects) {
		return ByteBuffer.wrap(serialize(objects));
	}

	private static boolean isTimeBased(UUID uuid) {
		return uuid.version() == 1;
	}

	@Override
	public int hashCode() {
		pack();
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(bytes);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Composite other = (Composite) obj;
		if (!Arrays.equals(this.serialize(), other.serialize())) {
			return false;
		}
		return true;
	}

}
